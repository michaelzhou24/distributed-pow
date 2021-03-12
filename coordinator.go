package distpow

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"net"
	"net/rpc"
	"sync"

	"github.com/DistributedClocks/tracing"
)

type WorkerAddr string

type WorkerClient struct {
	addr       WorkerAddr
	client     *rpc.Client
	workerByte uint8
}

type CoordinatorConfig struct {
	ClientAPIListenAddr string
	WorkerAPIListenAddr string
	Workers             []WorkerAddr
	TracerServerAddr    string
	TracerSecret        []byte
}

type CoordinatorMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
}

type CoordinatorWorkerMine struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

type CoordinatorWorkerResult struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
}

type CoordinatorWorkerCancel struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
}

type CoordinatorSuccess struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
}

type Coordinator struct {
	config  CoordinatorConfig
	tracer  *tracing.Tracer
	workers []*WorkerClient
}

/****** RPC structs ******/
type CoordMineArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	TraceToken       tracing.TracingToken
}

type CoordMineResponse struct {
	Nonce            []uint8
	NumTrailingZeros uint
	Secret           []uint8
	TraceToken       tracing.TracingToken
}

type CoordResultArgs struct {
	Nonce            []uint8
	NumTrailingZeros uint
	WorkerByte       uint8
	Secret           []uint8
	TraceToken       tracing.TracingToken
}

type ResultChan chan CoordResultArgs

type CoordRPCHandler struct {
	tracer     *tracing.Tracer
	workers    []*WorkerClient
	workerBits uint
	mineTasks  CoordinatorMineTasks
	mu         sync.Mutex
	cache      map[string][]uint8 // nonce->secret
	nonceMap   map[string]uint    // nonce->t
}

type RPCToken struct {
	TraceToken tracing.TracingToken
}

type CoordinatorMineTasks struct {
	mu    sync.Mutex
	tasks map[string]ResultChan
}

func NewCoordinator(config CoordinatorConfig) *Coordinator {
	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracerServerAddr,
		TracerIdentity: "coordinator",
		Secret:         config.TracerSecret,
	})

	workerClients := make([]*WorkerClient, len(config.Workers))
	for i, addr := range config.Workers {
		workerClients[i] = &WorkerClient{
			addr:       addr,
			client:     nil,
			workerByte: uint8(i),
		}
	}

	return &Coordinator{
		config:  config,
		tracer:  tracer,
		workers: workerClients,
	}
}

// Mine is a blocking RPC from powlib instructing the Coordinator to solve a specific pow instance
func (c *CoordRPCHandler) Mine(args CoordMineArgs, reply *CoordMineResponse) error {
	trace := c.tracer.ReceiveToken(args.TraceToken)
	trace.RecordAction(CoordinatorMine{
		NumTrailingZeros: args.NumTrailingZeros,
		Nonce:            args.Nonce,
	})

	// check cache before doing anything
	//cacheKey := byteSliceToString(args.Nonce)
	//if val, ok := c.cache[cacheKey]; ok {
	//log.Printf("Cache hit, checking if numtrailingzeroes matches %x\n", val)
	//if c.nonceMap[cacheKey] >= args.NumTrailingZeros {
	//log.Printf("Cache hit! Got secret %x with nonce %x.\n", args.Nonce, val)
	if sec := c.cacheContains(args.Nonce, args.NumTrailingZeros); sec != nil {
		trace.RecordAction(CacheHit{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			Secret:           sec,
		})
		reply.NumTrailingZeros = args.NumTrailingZeros
		reply.Nonce = args.Nonce
		reply.Secret = sec
		reply.TraceToken = trace.GenerateToken()
		trace.RecordAction(CoordinatorSuccess{
			Nonce:            reply.Nonce,
			NumTrailingZeros: reply.NumTrailingZeros,
			Secret:           reply.Secret,
		})
		return nil
		//}
	}
	trace.RecordAction(CacheMiss{
		Nonce:            args.Nonce,
		NumTrailingZeros: args.NumTrailingZeros,
	})

	// initialize and connect to workers (if not already connected)
	for err := initializeWorkers(c.workers); err != nil; {
		log.Println(err)
		err = initializeWorkers(c.workers)
	}

	workerCount := len(c.workers)

	resultChan := make(chan CoordResultArgs, workerCount)
	c.mineTasks.set(args.Nonce, args.NumTrailingZeros, resultChan)

	for _, w := range c.workers {
		trace.RecordAction(CoordinatorWorkerMine{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       w.workerByte,
		})
		args := WorkerMineArgs{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       w.workerByte,
			WorkerBits:       c.workerBits,
			TraceToken:       trace.GenerateToken(),
		}
		workerReply := RPCToken{}
		err := w.client.Call("WorkerRPCHandler.Mine", args, &workerReply)
		if err != nil {
			return err
		}
		c.tracer.ReceiveToken(workerReply.TraceToken)
	}

	// wait for at least one result
	result := <-resultChan
	// sanity check
	if result.Secret == nil {
		log.Fatalf("First worker result appears to be cancellation ACK, from workerByte = %d", result.WorkerByte)
	}
	// TODO: Deal with multiple results
	// after receiving one result, cancel all workers unconditionally.
	// the cancellation takes place of an ACK for any workers sending results.
	err := c.handleResults(args, result, trace, workerCount, resultChan)
	if err != nil {
		return err
	}

	// delete completed mine task from map
	c.mineTasks.delete(args.Nonce, args.NumTrailingZeros)

	reply.NumTrailingZeros = result.NumTrailingZeros
	reply.Nonce = result.Nonce
	reply.Secret = result.Secret
	trace.RecordAction(CoordinatorSuccess{
		Nonce:            reply.Nonce,
		NumTrailingZeros: reply.NumTrailingZeros,
		Secret:           reply.Secret,
	})
	reply.TraceToken = trace.GenerateToken()
	fmt.Println("cache state: ", c.cache)
	fmt.Println(c.nonceMap)
	return nil
}

func (c *CoordRPCHandler) handleResults(args CoordMineArgs, result CoordResultArgs, trace *tracing.Trace, workerCount int, resultChan chan CoordResultArgs) error {
	err := c.sendFoundResult(args, result, trace)
	if err != nil {
		return err
	}

	log.Printf("Waiting for %d acks from workers, then we are done", workerCount)

	// wait for all all workers to send back cancel ACK, ignoring results (receiving them is logged, but they have no further use here)
	// we asked all workers to cancel, so we should get exactly workerCount ACKs.
	workerAcksReceived := 0
	for workerAcksReceived < workerCount {
		ack := <-resultChan
		if ack.Secret == nil {
			log.Printf("Counting toward acks: %v", ack)
			workerAcksReceived += 1
		} else {
			// TODO: deal with extra result!
			log.Printf("Extra result!: %v", ack)
			if compare(ack.Nonce, ack.NumTrailingZeros, c.nonceMap, result.Secret, ack.Secret) {
				err := c.sendFoundResult(args, result, trace)
				if err != nil {
					return err
				}
				return nil
			}
		}
	}
	return nil
}

func (c *CoordRPCHandler) sendFoundResult(args CoordMineArgs, result CoordResultArgs, trace *tracing.Trace) error {
	for _, w := range c.workers {
		trace.RecordAction(CoordinatorWorkerCancel{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       w.workerByte,
		})
		args := WorkerCancelArgs{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       w.workerByte,
			TraceToken:       trace.GenerateToken(),
			Secret:           result.Secret,
		}
		reply := RPCToken{}
		err := w.client.Call("WorkerRPCHandler.Found", args, &reply)
		if err != nil {
			return err
		}
		c.tracer.ReceiveToken(reply.TraceToken)
	}
	return nil
}

// Result is a non-blocking RPC from the worker that sends the solution to some previous pow instance assignment
// back to the Coordinator
func (c *CoordRPCHandler) Result(args CoordResultArgs, reply *struct{}) error {
	trace := c.tracer.ReceiveToken(args.TraceToken)
	if args.Secret != nil {
		trace.RecordAction(CoordinatorWorkerResult{
			Nonce:            args.Nonce,
			NumTrailingZeros: args.NumTrailingZeros,
			WorkerByte:       args.WorkerByte,
			Secret:           args.Secret,
		})
		if sec := c.cacheContains(args.Nonce, args.NumTrailingZeros); sec != nil {
			trace.RecordAction(CacheHit{
				Nonce:            args.Nonce,
				NumTrailingZeros: args.NumTrailingZeros,
				Secret:           sec,
			})
		} else {
			trace.RecordAction(CacheMiss{
				Nonce:            args.Nonce,
				NumTrailingZeros: args.NumTrailingZeros,
			})
		}
		c.updateCache(trace, args.NumTrailingZeros, c.cache, c.nonceMap, args.Nonce, args.Secret)
	} else {
		log.Printf("Received worker cancel ack: %v", args)
	}
	c.mineTasks.get(args.Nonce, args.NumTrailingZeros) <- args // add to resultchan
	return nil
}

func (c *Coordinator) InitializeRPCs() error {
	handler := &CoordRPCHandler{
		tracer:     c.tracer,
		workers:    c.workers,
		workerBits: uint(math.Log2(float64(len(c.workers)))),
		mineTasks: CoordinatorMineTasks{
			tasks: make(map[string]ResultChan),
		},
		cache:    make(map[string][]uint8),
		nonceMap: make(map[string]uint),
	}
	server := rpc.NewServer()
	err := server.Register(handler) // publish Coordinator<->worker procs
	if err != nil {
		return fmt.Errorf("format of Coordinator RPCs aren't correct: %s", err)
	}

	workerListener, e := net.Listen("tcp", c.config.WorkerAPIListenAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", c.config.WorkerAPIListenAddr, e)
	}

	clientListener, e := net.Listen("tcp", c.config.ClientAPIListenAddr)
	if e != nil {
		return fmt.Errorf("failed to listen on %s: %s", c.config.ClientAPIListenAddr, e)
	}

	go server.Accept(workerListener)
	server.Accept(clientListener)

	return nil
}

func initializeWorkers(workers []*WorkerClient) error {
	for _, w := range workers {
		if w.client == nil {
			client, err := rpc.Dial("tcp", string(w.addr))
			if err != nil {
				log.Printf("Waiting for worker %d", w.workerByte)
				return fmt.Errorf("failed to dial worker: %s", err)
			}
			w.client = client
		}
	}
	return nil
}

func compare(nonce []uint8, t2 uint, nonceMap map[string]uint, s1 []uint8, s2 []uint8) bool {
	return t2 > nonceMap[byteSliceToString(nonce)] ||
		(t2 == nonceMap[byteSliceToString(nonce)] && bytes.Compare(s2, s1) > 0)
}

// nil if not in cache
// return secret in cache otherwise
func (c *CoordRPCHandler) cacheContains(nonce []uint8, numTrailingZeroes uint) []uint8 {
	c.mu.Lock()
	defer c.mu.Unlock()
	cacheKey := byteSliceToString(nonce)
	if val, ok := c.cache[cacheKey]; ok {
		if t, ok2 := c.nonceMap[cacheKey]; ok2 && t >= numTrailingZeroes {
			return val
		}
	}
	return nil
}

/*
- Update the cache when the a worker sends a result back to the coordinator.
- Remove cache entry with (n1, t) if an entry (n1, t+1) is added.
*/
func (c *CoordRPCHandler) updateCache(trace *tracing.Trace, numTrailingZeroes uint, cache map[string][]uint8, nonceMap map[string]uint, nonce []uint8, secret []uint8) {
	cacheKey := byteSliceToString(nonce)
	//log.Printf("Secret given: %x", secret)
	//trailingZeroes := getNumTrailingZeroes(nonce, secret)
	c.mu.Lock()
	defer c.mu.Unlock()
	if val, ok := cache[cacheKey]; ok {
		if compare(nonce, numTrailingZeroes, nonceMap, val, secret) {
			trace.RecordAction(CacheRemove{
				Nonce:            nonce,
				NumTrailingZeros: nonceMap[byteSliceToString(nonce)],
				Secret:           cache[cacheKey],
			})
			trace.RecordAction(CacheAdd{
				Nonce:            nonce,
				NumTrailingZeros: numTrailingZeroes,
				Secret:           secret,
			})
			nonceMap[cacheKey] = numTrailingZeroes
			cache[cacheKey] = secret
		}
	} else {
		trace.RecordAction(CacheAdd{
			Nonce:            nonce,
			NumTrailingZeros: numTrailingZeroes,
			Secret:           secret,
		})
		nonceMap[cacheKey] = numTrailingZeroes
		cache[cacheKey] = secret
	}
	fmt.Println("Cache state: ", cache)
	fmt.Println(nonceMap)
}

func (t *CoordinatorMineTasks) get(nonce []uint8, numTrailingZeros uint) ResultChan {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.tasks[generateCoordTaskKey(nonce, numTrailingZeros)]
}

func (t *CoordinatorMineTasks) set(nonce []uint8, numTrailingZeros uint, val ResultChan) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tasks[generateCoordTaskKey(nonce, numTrailingZeros)] = val
	log.Printf("New task added: %v\n", t.tasks)
}

func (t *CoordinatorMineTasks) delete(nonce []uint8, numTrailingZeros uint) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.tasks, generateCoordTaskKey(nonce, numTrailingZeros))
	log.Printf("Task deleted: %v\n", t.tasks)
}

func generateCoordTaskKey(nonce []uint8, numTrailingZeros uint) string {
	return fmt.Sprintf("%s|%d", hex.EncodeToString(nonce), numTrailingZeros)
}

func byteSliceToString(nonce []uint8) string {
	return fmt.Sprintf("%s", hex.EncodeToString(nonce))
}

func getCacheKey(nonce []uint8, numTrailingZeroes uint) string {
	return fmt.Sprintf("%s___%d", byteSliceToString(nonce), numTrailingZeroes)
}

func getNumTrailingZeroes(nonce []uint8, secret []uint8) uint {
	hashStrBuf := new(bytes.Buffer)
	secretBuf := new(bytes.Buffer)
	if _, err := secretBuf.Write(nonce); err != nil {
		panic(err)
	}
	if _, err := secretBuf.Write(secret); err != nil {
		panic(err)
	}
	hash := md5.Sum(secretBuf.Bytes())
	fmt.Fprintf(hashStrBuf, "%x", hash)
	//fmt.Printf("hashed to: %x\n", hash)
	str := hashStrBuf.Bytes()
	var trailingZeroesFound uint
	for i := len(str) - 1; i >= 0; i-- {
		if str[i] == '0' {
			trailingZeroesFound++
		} else {
			break
		}
	}
	return trailingZeroesFound
}
