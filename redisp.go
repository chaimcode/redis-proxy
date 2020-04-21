package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"redisp/redcon"

	"github.com/go-redis/redis"
)

var (
	flagH       bool
	proxyAddr   string
	sourceAddr  string
	targetAddr  string
	limitMemory int

	err error
	mu  sync.RWMutex
)

func init() {
	flag.BoolVar(&flagH, "h", false, "this help")
	flag.StringVar(&proxyAddr, "p", "localhost:6379", "proxy addr")
	flag.StringVar(&sourceAddr, "s", "source.com:6379", "source redis address")
	flag.StringVar(&targetAddr, "t", "target.com:6379", "target redis address")
	flag.IntVar(&limitMemory, "l", 0, "artificially limit the maximum memory")
	flag.Usage = usage
}

func main() {
	flag.Parse()

	if flagH {
		flag.Usage()
		return
	}

	sourceClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{sourceAddr},
		Password: "", // no password set
	})
	targetClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{targetAddr},
		Password: "", // no password set
	})

	go log.Printf("started server at %s \nsource: %s\ntarget: %s\n", proxyAddr, sourceAddr, targetAddr)

	parallelMigrate(*sourceClient, *targetClient)

	err = redcon.ListenAndServe(proxyAddr,
		func(conn redcon.Conn, cmd redcon.Command) {
			// TODO: 上线前调到 default 中
			cmdStr := ""
			for _, b := range cmd.Args {
				cmdStr += " " + string(b)
			}
			go log.Println("cmd: ", cmdStr)
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + cmdStr + "'")
			case "detach":
				hconn := conn.Detach()
				log.Printf("connection has been detached")
				go func() {
					defer hconn.Close()
					hconn.WriteString("OK")
					hconn.Flush()
				}()
				return
			case "ping":
				conn.WriteString("PONG")
			case "info":
				if len(cmd.Args) > 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				var (
					val string
					ok  error
				)
				if len(cmd.Args) == 2 {
					section := string(cmd.Args[1])
					val, ok = sourceClient.Info(section).Result()

				} else {
					val, ok = sourceClient.Info().Result()
				}
				if ok != nil {
					conn.WriteError(ok.Error())
					return
				}
				conn.WriteString(string(val))
			case "cluster":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				slots, ok := sourceClient.ClusterSlots().Result()
				if ok != nil {
					conn.WriteError(ok.Error())
					return
				}
				val := ""
				for i, slot := range slots {
					itemVal := ""
					itemVal += fmt.Sprintf("%d) 1) (integer) %d\r\n", i+1, slot.Start)
					itemVal += fmt.Sprintf("   2) (integer) %d\r\n", slot.End)
					for j, node := range slot.Nodes {
						addr := strings.Split(node.Addr, ":")
						itemVal += fmt.Sprintf("   %d) 1) \"%s\"\r\n", j+3, addr[0])
						itemVal += fmt.Sprintf("      2) (integer) %s\r\n", addr[1])
						itemVal += fmt.Sprintf("      3) \"%s\"\r\n", node.Id)
					}
					val += itemVal
				}
				conn.WriteBulkString(val)
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.Lock()
				key, val, duration := string(cmd.Args[1]), cmd.Args[2], 0*time.Second
				err = sourceClient.Set(key, val, duration).Err()
				if err == nil {
					err = targetClient.Set(key, val, duration).Err()
				}
				mu.Unlock()
				if err != nil {
					conn.WriteNull()
					return
				}
				conn.WriteString("OK")
			case "get":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.RLock()
				key := string(cmd.Args[1])
				val, ok := targetClient.Get(key).Result()
				if val == "" {
					val, ok = sourceClient.Get(key).Result()
					duration, _ := sourceClient.TTL(key).Result()
					targetClient.Set(key, val, duration)

				}
				mu.RUnlock()
				if ok != nil {
					conn.WriteNull()
					return
				}
				conn.WriteString(val)
			case "del":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.Lock()
				key := string(cmd.Args[1])
				val, ok := sourceClient.Del(key).Result()
				targetClient.Del(key).Result()
				mu.Unlock()
				if ok != nil {
					conn.WriteError(ok.Error())
					return
				}
				conn.WriteInt(int(val))
			case "expire":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.Lock()
				key := string(cmd.Args[1])
				durationInt, err := strconv.Atoi(string(cmd.Args[2]))
				if err != nil {
					conn.WriteError(err.Error())
					return
				}
				duration := time.Duration(time.Duration(durationInt) * time.Second)
				val, ok := sourceClient.Expire(key, duration).Result()
				targetClient.Expire(key, duration).Result()
				mu.Unlock()
				if ok != nil {
					conn.WriteNull()
					return
				}
				if !val {
					conn.WriteInt(0)
					return
				}
				conn.WriteInt(1)
			case "exists":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				mu.Lock()
				key := string(cmd.Args[1])
				val, ok := sourceClient.Exists(key).Result()
				mu.Unlock()
				if ok != nil {
					conn.WriteNull()
					return
				}
				conn.WriteInt(int(val))
			}
		},
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			go log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			go log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr,
		`redisp version: redisp/0.1.0
Usage: redisp  [-s source] [-t target]

Options:
`)
	flag.PrintDefaults()
}

func parallelMigrate(sourceClient, targetClient redis.ClusterClient) {
	nodes, _ := sourceClient.ClusterNodes().Result()
	addrRegexp, _ := regexp.Compile(`((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})(\.((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})){3}:\d{4,5}`)
	addrs := addrRegexp.FindAllString(nodes, -1)
	for i, addr := range addrs {
		sourceNodeClient := redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: "", // no password set
			DB:       0,  // use default DB
		})
		go log.Println("node", i, "addr:", addr)
		go migrate(sourceNodeClient, targetClient)
	}
}

func migrate(sourceClient *redis.Client, targetClient redis.ClusterClient) {
	var (
		page   []string
		cursor uint64
		err    error
	)
	cursor = 0
	for {
		page, cursor, err = sourceClient.Scan(cursor, "*", 1000).Result()
		if err != nil {
			log.Println(err.Error())
		}
		go log.Println("cursor:", cursor)
		for _, key := range page {
			mu.Lock()
			val, _ := sourceClient.Get(key).Result()
			duration, _ := sourceClient.TTL(key).Result()
			targetClient.Set(key, val, duration)
			mu.Unlock()

		}
		val, _ := targetClient.Info("Memory").Result()
		r, _ := regexp.Compile(".*used_memory:(.*).*")
		used, _ := strconv.Atoi(strings.TrimSpace(strings.Split(r.FindString(val), ":")[1]))
		go log.Println("info Memory:", used)
		if cursor <= 0 || (limitMemory > 0 && used > limitMemory) {
			break
		}
	}
}
