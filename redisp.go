package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	redcon "github.com/tidwall/redcon"
)

var (
	flagH       bool
	proxyAddr   string
	sourceAddr  string
	targetAddr  string
	limitMemory int

	err error
)

type cmdStrcut struct {
	cmd  *redcon.Command
	conn *redcon.Conn
}

func init() {
	flag.BoolVar(&flagH, "h", false, "this help")
	flag.StringVar(&proxyAddr, "p", "localhost:6380", "proxy addr")
	flag.StringVar(&sourceAddr, "s", "127.0.0.1:6379", "source redis address")
	flag.StringVar(&targetAddr, "t", "127.0.0.1:6379", "target redis address")
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

	log.Printf("started server at %s \nsource: %s\ntarget: %s\n", proxyAddr, sourceAddr, targetAddr)

	// parallelMigrate(*sourceClient, *targetClient)
	// test := func(conn *redcon.Conn) {
	// 	con := *conn
	// 	con.WriteString("kkkkk")
	// }

	err := redcon.ListenAndServe(proxyAddr,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				cmdStr := ""
				for _, b := range cmd.Args {
					cmdStr += " " + string(b)
				}
				log.Println("cmd: ", cmdStr)
				conn.WriteError("ERR unknown command '" + cmdStr + "'")
			case "detach":
				hconn := conn.Detach()
				log.Printf("connection has been detached")
				defer hconn.Close()
				hconn.WriteString("OK")
				hconn.Flush()
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
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				key, val, duration := string(cmd.Args[1]), cmd.Args[2], 0*time.Second
				err = sourceClient.Set(key, val, duration).Err()
				if err == nil {
					targetClient.Set(key, val, duration)
				}
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
				key := string(cmd.Args[1])
				val, ok := targetClient.Get(key).Result()
				if val == "" {
					val, ok = sourceClient.Get(key).Result()
					duration, _ := sourceClient.TTL(key).Result()
					targetClient.Set(key, val, duration)

				}
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

				key := string(cmd.Args[1])
				val, ok := sourceClient.Del(key).Result()
				targetClient.Del(key).Result()

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

				key := string(cmd.Args[1])
				durationInt, err := strconv.Atoi(string(cmd.Args[2]))
				if err != nil {
					conn.WriteError(err.Error())
					return
				}
				duration := time.Duration(time.Duration(durationInt) * time.Second)
				val, ok := sourceClient.Expire(key, duration).Result()
				targetClient.Expire(key, duration).Result()

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

				key := string(cmd.Args[1])
				val, ok := sourceClient.Exists(key).Result()

				if ok != nil {
					conn.WriteNull()
					return
				}
				conn.WriteInt(int(val))
			}
		},
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
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
