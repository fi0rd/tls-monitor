package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/go-redis/redis/v8"
	"gopkg.in/yaml.v2"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// redis value:
//     "targets": ["https://<hostname>"],
//     "labels": {
//         "datacenter": "",
//         "namespace": "",
//         "ip": ""

type OneCloudService struct {
	Name      string
	Namespace string
	State     string
	Submitter string
	Updater   string
}

type OneCloudManifestService struct {
	Queue string            `json:"queue"`
	Ports map[string]string `json:"ports"`
}

type SysUser struct {
	Email string `json:"email"`
}

type ResponseUser struct {
	Objects []SysUser `json:"objects"`
}

type Group struct {
	Targets      []string          `json:"targets"`         // targets for prometheus
	Labels       map[string]string `json:"labels"`          // blackbox exporter labels
	Alive        bool              `json:"alive"`           // alive status
	CheckSSLByIP bool              `json:"check_ssl_by_ip"` // check ssl by ip if not possible by hostname
}

type Config struct {
	OneCloudServiceUrl         string        `yaml:"OneCloudServiceUrl"`
	OneCloudServiceManifestUrl string        `yaml:"OneCloudServiceManifestUrl"`
	NumWorkers                 int           `yaml:"NumWorkers"`
	RedisHost                  string        `yaml:"RedisHost"`
	RedisUsername              string        `yaml:"RedisUsername"`
	RedisPassword              string        `yaml:"RedisPassword"`
	RedisTTL                   time.Duration `yaml:"RedisTTL"`
	LogFile                    string        `yaml:"LogFile"`
	VKTeamChatId               string        `yaml:"VKTeamChatId"`
	SysUsersUrl                string        `yaml:"SysUsersUrl"`
	SysApiApp                  string        `yaml:"SysApiApp"`
	SysApiKey                  string        `yaml:"SysApiKey"`
	OneCloudApiCertPrivateKey  string        `yaml:"OneCloudApiCertPrivateKey"`
	OneCloudApiCertPEM         string        `yaml:"OneCloudApiCertPEM"`
	NonGwanDatacenters         []string      `yaml:"NonGwanDatacenters"`
}

var config Config
var configFile string

func init() {
	// Load configuration
	flag.StringVar(&configFile, "config", "../../configs/targets_fetcher.yml", "Path to the configuration file")
	flag.Parse()
	var err error
	config, err = loadConfig(configFile)
	if err != nil {
		log.Fatalf("error loading config: %v", err)
	}

	logFile, errLogFile := os.OpenFile(config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if errLogFile != nil {
		log.Fatalf("error opening log file: %v", errLogFile)
	}
	log.SetOutput(logFile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile) // Set log format
}

func checkAlive(g *Group) bool {
	const (
		httpsPort   = 443
		maxRetries  = 2
		dialTimeout = 2 * time.Second
		retryDelay  = 1 * time.Second
	)

	checkConnection := func(address, serverName string) bool {
		dialer := &net.Dialer{Timeout: dialTimeout}
		conn, err := tls.DialWithDialer(dialer, "tcp", address, &tls.Config{
			ServerName:         serverName,
			InsecureSkipVerify: true,
		})
		if err != nil {
			//log.Printf("Connection error to %s (SNI: %s): %v", address, serverName, err)
			return false
		}
		defer conn.Close()
		return true
	}

	retryCheck := func(address, serverName string) bool {
		for i := 0; i < maxRetries; i++ {
			if checkConnection(address, serverName) {
				return true
			}
			if i < maxRetries-1 {
				time.Sleep(retryDelay)
			}
		}
		return false
	}

	hostnameURL := fmt.Sprintf("%s:%v", g.Labels["hostname"], httpsPort)
	ipAddressPort := fmt.Sprintf("%s:%v", g.Labels["ip"], httpsPort)

	if retryCheck(hostnameURL, g.Labels["hostname"]) {
		g.Alive = true
		g.CheckSSLByIP = false
		log.Printf("host is alive via hostname: %s", hostnameURL)
		return true
	}

	// if connection by hostname failed - check by ip
	if checkConnection(ipAddressPort, g.Labels["hostname"]) {
		g.Alive = true
		g.CheckSSLByIP = true
		log.Printf("host is alive via IP: %s", ipAddressPort)
		return true
	}

	g.Alive = false
	return false
}

func getDataByAPIRequest(url string, header http.Header, objectDict interface{}, sslVerify bool) error {
	var httpClient http.Client
	if sslVerify == true {
		cert, err := tls.X509KeyPair([]byte(config.OneCloudApiCertPEM), []byte(config.OneCloudApiCertPrivateKey))
		if err != nil {
			log.Printf("failed to load certificate and private key: %v", err)
			return err
		}
		httpClient = http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					Certificates: []tls.Certificate{cert},
				},
			},
		}
	} else {
		httpClient = http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, errCreateRequest := http.NewRequestWithContext(ctx, "GET", url, nil)

	if errCreateRequest != nil {
		log.Printf("failed to create http request: %v", errCreateRequest)
		return errCreateRequest
	}

	if header != nil {
		req.Header = header
	}

	response, errDoRequest := httpClient.Do(req)

	if errDoRequest != nil {
		log.Printf("failed to send http request %s : %v", url, errDoRequest)
		return errDoRequest
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("failed to close body: %v", err)
		}
	}(response.Body)
	if response.StatusCode != http.StatusOK {
		log.Printf("error fetching data from %s: status code %d", url, response.StatusCode)
		return fmt.Errorf("error fetching data from %s: status code %d", url, response.StatusCode)
	}
	if err := json.NewDecoder(response.Body).Decode(objectDict); err != nil {
		log.Printf("failed to decode response: %v", err)
		return err
	}
	return nil
}

func getChatIdBySubmitter(login string, config Config, redisClientUsers *redis.Client) (string, error) {
	var responseBodyUser ResponseUser
	noHeader := make(http.Header)
	chatId := ""
	if !strings.Contains(strings.ToLower(login), ".") {
		return "", fmt.Errorf("system submitter, no need to check chatid")
	}
	ctxRedis, cancelCtx := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCtx()
	chatIdUserCached, err := redisClientUsers.Get(ctxRedis, login).Result()
	if err != nil {
		log.Printf("error read user's chatid for %s from cache, do request to SYS API: %v", login, err)
	}
	if chatIdUserCached != "" {
		return chatIdUserCached, nil
	}
	SysUsersUrl := strings.ToLower(config.SysUsersUrl)
	SysUsersUrl = strings.Replace(SysUsersUrl, "<username>", login, 1)
	if errAPIRequestUser := getDataByAPIRequest(SysUsersUrl, noHeader, &responseBodyUser, false); errAPIRequestUser != nil {
		log.Printf("failed to fetch data from %s: %v", SysUsersUrl, errAPIRequestUser)
	}
	if len(responseBodyUser.Objects) > 0 {
		chatId = responseBodyUser.Objects[0].Email

		ctxRedis, cancelCtxRedis := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelCtxRedis()
		if err := redisClientUsers.Set(ctxRedis, login, chatId, 24*30*time.Hour).Err(); err != nil {
			return "", fmt.Errorf("failed to set user chatid in cache: %v", err)
		}
	}
	return chatId, nil
}

func richServiceData(group Group, config Config, count *int32, redisClientTargets *redis.Client, redisClientUsers *redis.Client) {
	if len(group.Targets) == 0 {
		log.Printf("no targets: %s : %s\n", group.Targets, group.Labels)
		return
	}

	target := group.Targets[0]
	hostname := group.Labels["hostname"]

	hostnameSplit := strings.Split(hostname, ".")
	serviceName := strings.Join(hostnameSplit[1:len(hostnameSplit)-3], ".")

	if serviceName == "" {
		log.Printf("service name is empty for target: %s", group.Labels["hostname"])
		return
	}

	headerService := http.Header{
		"Accept":                {"application/json"},
		"X-One-Cloud-Namespace": {group.Labels["namespace"]},
	}

	OneCloudServiceURL := strings.ToLower(config.OneCloudServiceUrl)
	OneCloudServiceURL = strings.Replace(OneCloudServiceURL, "<service>", serviceName, 1)
	OneCloudServiceURL = strings.Replace(OneCloudServiceURL, "<datacenter>", group.Labels["datacenter"], 1)

	OneCloudServiceManifestURL := strings.ToLower(config.OneCloudServiceManifestUrl)
	OneCloudServiceManifestURL = strings.Replace(OneCloudServiceManifestURL, "<service>", serviceName, 1)
	OneCloudServiceManifestURL = strings.Replace(OneCloudServiceManifestURL, "<datacenter>", group.Labels["datacenter"], 1)

	// in HC and CC we need to remove gwan from the url
	if slices.Contains(config.NonGwanDatacenters, group.Labels["datacenter"]) {
		OneCloudServiceURL = strings.Replace(OneCloudServiceURL, "gwan.", "", 1)
		OneCloudServiceManifestURL = strings.Replace(OneCloudServiceManifestURL, "gwan.", "", 1)
	}

	// check target's service manifest on 443 port
	var responseBodyManifestService OneCloudManifestService

	if errAPIRequestManifestService := getDataByAPIRequest(OneCloudServiceManifestURL, headerService, &responseBodyManifestService, true); errAPIRequestManifestService == nil {
		group.Labels["queue"] = strings.ToLower(responseBodyManifestService.Queue)
	}

	// rich prometheus target by service data (submitter)
	var responseBodyService OneCloudService
	if errAPIRequestService := getDataByAPIRequest(OneCloudServiceURL, headerService, &responseBodyService, false); errAPIRequestService != nil {
		log.Printf("failed to fetch data from %s: %v", OneCloudServiceURL, errAPIRequestService)
	} else {
		// rich prometheus target by service data (chat id)
		chatId, err := getChatIdBySubmitter(responseBodyService.Submitter, config, redisClientUsers)
		if err != nil {
			chatId = config.VKTeamChatId
		} else if chatId == "" {
			log.Printf("%s: non-human submitter, alarm will send to the common chat: %s\n", target, config.VKTeamChatId)
		}
		chatId = config.VKTeamChatId // send alarm to common chat, removed in prod if you need send to personal
		group.Labels["chat_id"] = chatId
	}

	group.Labels["name"] = strings.ToLower(responseBodyService.Name)
	group.Labels["namespace"] = strings.ToLower(responseBodyService.Namespace)
	group.Labels["state"] = strings.ToLower(responseBodyService.State)
	group.Labels["submitter"] = strings.ToLower(responseBodyService.Submitter)
	group.Labels["updater"] = strings.ToLower(responseBodyService.Updater)

	var targets []string

	if group.CheckSSLByIP {
		targets = []string{"https://" + group.Labels["ip"]}
	} else {
		targets = []string{"https://" + group.Labels["hostname"]}
	}

	service := &Group{
		Targets: targets,
		Labels:  group.Labels,
		Alive:   group.Alive,
	}
	cachedServiceJSON, errJsonMarshal := json.Marshal(service)
	if errJsonMarshal != nil {
		log.Printf("failed to marshal service for cache: %v", errJsonMarshal)
		return
	}

	ctxRedisSet, cancelCtxRedisSet := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCtxRedisSet()

	if err := redisClientTargets.Set(
		ctxRedisSet,
		target,
		cachedServiceJSON,
		config.RedisTTL*time.Second,
	).Err(); err != nil {
		log.Printf("failed to set service in cache: %v", err)
		return
	}
	atomic.AddInt32(count, 1)
}

func getCacheRecords(redisClientTargets *redis.Client) ([]Group, error) {
	startTime := time.Now()
	ctxRedisScan, cancelRedisScan := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelRedisScan()

	iter := redisClientTargets.Scan(ctxRedisScan, 0, "*", 0).Iterator()

	var keys []string
	for iter.Next(ctxRedisScan) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("redis keys scan error: %w", err)
	}

	log.Printf("Found %d keys in %v\n", len(keys), time.Since(startTime))

	if len(keys) == 0 {
		return []Group{}, nil
	}

	const batchSize = 200
	records := make([]Group, 0, len(keys))

	ctxRedisGet, cancelRedisGet := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelRedisGet()

	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}

		currentBatch := keys[i:end]

		// Use pipelining for batch retrieval
		pipe := redisClientTargets.Pipeline()
		targets := make(map[string]*redis.StringCmd, len(currentBatch))

		for _, key := range currentBatch {
			targets[key] = pipe.Get(ctxRedisGet, key)
		}
		_, err := pipe.Exec(ctxRedisGet)

		if err != nil {
			return nil, fmt.Errorf("failed to execute Redis pipeline (batch %d-%d): %w", i, end-1, err)
		}

		// Process batch results
		for key, target := range targets {
			cachedGroup, err := target.Result()
			if errors.Is(err, redis.Nil) {
				// Key doesn't exist, skip
				log.Printf("target key not found in redis: %v", key)
				continue
			} else if err != nil {
				log.Printf("failed to get Redis key %v: %v", key, err)
				continue
			}
			var group Group
			if err := json.Unmarshal([]byte(cachedGroup), &group); err != nil {
				log.Printf("failed to unmarshal Group for key %v: %v", key, err)
				continue
			}
			records = append(records, group)
		}
	}
	return records, nil
}

func loadConfig(filename string) (Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return config, fmt.Errorf("error opening config file: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("error closing file: %v", err)
		}
	}(file)

	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return config, fmt.Errorf("error decoding config file: %w", err)
	}

	if config.RedisHost == "" {
		config.RedisHost = "localhost"
	}
	return config, nil
}

func main() {
	startTime := time.Now()
	log.Printf("Workers: %d\n", config.NumWorkers)

	redisClientTargets := redis.NewClient(&redis.Options{
		Addr:         config.RedisHost + ":6379",
		Username:     config.RedisUsername,
		Password:     config.RedisPassword,
		PoolSize:     config.NumWorkers + 50,
		MinIdleConns: config.NumWorkers,
		ReadTimeout:  10 * time.Second,
		DB:           0,
	})

	redisClientUsers := redis.NewClient(&redis.Options{
		Addr:         config.RedisHost + ":6379",
		Username:     config.RedisUsername,
		Password:     config.RedisPassword,
		PoolSize:     config.NumWorkers + 50,
		MinIdleConns: config.NumWorkers,
		ReadTimeout:  10 * time.Second,
		DB:           1,
	})
	if err := redisClientTargets.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("error connecting to redis: %v", err)
	}

	defer func() {
		redisClientTargets.Close()
		redisClientUsers.Close()
	}()

	targetGroups, err := getCacheRecords(redisClientTargets)
	if err != nil {
		log.Fatalf("error getting DNS records: %v", err)
	}

	if len(targetGroups) == 0 {
		log.Fatalf("no records found in cache")
	}

	log.Printf("Execution Redis time: %v\n", time.Now().Sub(startTime))
	log.Printf("Total service URLs: %d\n", len(targetGroups))

	wg := sync.WaitGroup{}
	semaphore := make(chan struct{}, config.NumWorkers) // Semaphore to limit concurrent goroutines
	var goroutineCount int32

	for _, group := range targetGroups {
		if _, exists := group.Labels["cluster"]; exists {
			continue
		}
		wg.Add(1)
		semaphore <- struct{}{} // Acquire a permit from the semaphore
		localGroup := group

		go func(g Group) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Recovered from panic in group %s: %v", g.Labels["hostname"], r)
				}
				<-semaphore
				wg.Done()
			}()

			if checkAlive(&g) {
				richServiceData(g, config, &goroutineCount, redisClientTargets, redisClientUsers)
			}
		}(localGroup)
	}

	wg.Wait()

	log.Printf("Total online services: %d\n", goroutineCount)
	log.Printf("Execution total time: %v\n", time.Since(startTime).Abs())
}
