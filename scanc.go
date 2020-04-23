package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"sync"

	"github.com/go-redis/redis"
)

var (
	flagH       bool
	proxyAddr   string
	sourceAddr  string
	targetAddr  string
	limitMemory int

	err error
)

func init() {
	flag.BoolVar(&flagH, "h", false, "this help")
	flag.StringVar(&proxyAddr, "p", "localhost:6380", "proxy addr")
	flag.StringVar(&sourceAddr, "s", "localhost:6379", "source redis address")
	flag.StringVar(&targetAddr, "t", "localhost:6379", "target redis address")
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

	clusterMigrate(sourceClient, targetClient)
}

func usage() {
	fmt.Fprintf(os.Stderr,
		`redisp version: redisp/0.1.0
Usage: redisp  [-s source] [-t target]

Options:
`)
	flag.PrintDefaults()
}

func clusterMigrate(sourceClient, targetClient *redis.ClusterClient) {
	var wg sync.WaitGroup
	nodes, _ := sourceClient.ClusterNodes().Result()
	addrRegexp, _ := regexp.Compile(`((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})(\.((2(5[0-5]|[0-4]\d))|[0-1]?\d{1,2})){3}:\d{4,5}`)
	addrs := addrRegexp.FindAllString(nodes, -1)
	for i, addr := range addrs {
		sourceNodeClient := redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: "", // no password set
			DB:       0,  // use default DB
		})
		log.Println("node", i, "addr:", addr)
		wg.Add(1)

		go func() {
			var (
				page   []string
				cursor uint64
				err    error
				wg     sync.WaitGroup
			)
			cursor = 0
			pageChan := make(chan []string, 300000)
			for {
				page, cursor, err = sourceNodeClient.Scan(cursor, "*", 1000).Result()
				if err != nil {
					log.Println(err.Error())
				}
				log.Println("cursor:", cursor)
				pageChan <- page
				break
				if cursor <= 0 {
					break
				}
			}

			go func() {
				wg.Add(1)
				page := <-pageChan
				for _, key := range page {
					log.Println("key", key)

					val, ok := sourceNodeClient.Get(key).Result()
					if ok != nil {
						continue
					}
					duration, _ := sourceNodeClient.TTL(key).Result()
					targetClient.Set(key, val, duration)
				}
				defer wg.Done()
			}()

			defer close(pageChan)
			defer wg.Done()
		}()

	}

	wg.Wait()
	log.Println("congratulation, migrate done ...")
}

// func nodeMigrate(sourceClient *redis.Client, targetClient *redis.ClusterClient) {
// 	var (
// 		page   []string
// 		cursor uint64
// 		err    error
// 		wg     sync.WaitGroup
// 	)
// 	cursor = 0
// 	pageChan := make(chan []string, 300000)
// 	for {
// 		page, cursor, err = sourceClient.Scan(cursor, "*", 1000).Result()
// 		if err != nil {
// 			log.Println(err.Error())
// 		}
// 		log.Println("cursor:", cursor)
// 		pageChan <- page
// 		if cursor <= 0 {
// 			break
// 		}
// 	}

// 	go func() {
// 		wg.Add(1)
// 		page := <-pageChan
// 		for _, key := range page {
// 			log.Println("key", key)

// 			val, ok := sourceClient.Get(key).Result()
// 			if ok != nil {
// 				continue
// 			}
// 			duration, _ := sourceClient.TTL(key).Result()
// 			targetClient.Set(key, val, duration)
// 		}
// 	}()

// 	wg.Wait()

// 	log.Println("congratulation, migrate done ...")

// 	defer close(pageChan)
// }
