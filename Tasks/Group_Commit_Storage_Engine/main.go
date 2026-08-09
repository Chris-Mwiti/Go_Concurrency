package main

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

type GroupCommitNode struct {
	Name string
	buffer *bytes.Buffer
	mu sync.Mutex
	cond *sync.Cond
	dataCh chan []byte

	//network
	proto string
	addr string
	l net.Listener


	//suppl
	logger *slog.Logger
}

type Client struct {
	conn net.Conn
	logger *slog.Logger
	cond *sync.Cond
}

func (c *Client) HandleConn(ctx context.Context, sendCh chan <-[]byte) (error) {

	//defer client connection closing
	defer c.conn.Close()
	c.cond.L.Lock()
	defer c.cond.L.Unlock()
	buf := new(bytes.Buffer)
	for {
		if err := ctx.Err(); err != nil {
			c.logger.ErrorContext(ctx, "context done", "err", err)
			return err
		}
		//@todo: I dont know if this is okay to do...
		buf.Reset()
		_, err := c.conn.Read(buf.AvailableBuffer())
		if err != nil {
			c.logger.ErrorContext(ctx, "error while reading conn", "err", err.Error())
			return err
		}

		//send the data over the channel
		sendCh <- buf.Bytes()

		//block the connection until to receive a signal to resume
		c.cond.Wait()

		c.logger.InfoContext(ctx, "complete connection")
		//so for here I don't know what will be the terminater so that I know its an EOF
		//that will lead to a break out of loop scenario
	}
}


func NewNode(name string, proto, addr string, logger *slog.Logger) (*GroupCommitNode, error){
	node := GroupCommitNode{
		Name: name,
		buffer: new(bytes.Buffer),	
		dataCh: make(chan []byte),
	}
	node.cond = sync.NewCond(&node.mu)
	node.logger = logger
	
	//establish the network ports
	node.proto = proto
	node.addr = addr
	l, err := net.Listen(proto, addr)
	if err != nil {
		return nil, err
	}

	node.l = l

	return &node, nil
} 

func (n *GroupCommitNode) BatchWrite(ctx context.Context,  interval time.Duration)(error) {
	ticker := time.NewTicker(interval)

	//destination file creation
	fd, err := os.OpenFile("sample.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		n.logger.ErrorContext(ctx, "open file opertion failed", "err", err.Error())
		return fmt.Errorf("error while creating sample file")
	}
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done")
		case <-ticker.C:
			n.cond.L.Lock()
		 for data := range n.dataCh {
			//perform a write operation to disk using fsync	
			_,err := fd.Write(data)
			if err != nil {
				n.logger.ErrorContext(ctx, "writing to file has failed", "err", err.Error())
				return fmt.Errorf("error while writing to sample file")
			}
		 }		
		 n.cond.Broadcast()
		 n.cond.L.Unlock()
		}
	}
}

func (n *GroupCommitNode) Listen(ctx context.Context) (error) {
	//spawn a new go routine to handle connections -> but this time create a client and attach a conn to handle those connections
	defer n.l.Close()
	for {

		if err := ctx.Err(); err != nil {
			n.logger.ErrorContext(ctx, "context done", "err", err.Error())
			return err
		}

		conn, err := n.l.Accept()
		if err != nil {
			//log the error but continue to accept incoming connectoins
			n.logger.WarnContext(ctx, "warning, error while accepting connection", "err", err.Error())
			continue
		}

		client := Client{conn: conn, logger: n.logger, cond: n.cond}
		//establish a goroutine to handle client connections
		go func (ctx context.Context, client Client){
			//create a client handler
			timeOutCtx, cancel := context.WithTimeout(ctx, 15 * time.Second)
			defer cancel()
			if err := client.HandleConn(timeOutCtx, n.dataCh); err != nil {
				n.logger.ErrorContext(ctx, "error while handling client conn", "err", err.Error())
				return
			}
		}(ctx, client)
	}
}

func main(){

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{}))
	node, err:= NewNode("group_commit_node", "tcp", "0.0.0.0:3224", logger)
	ctx := context.Background()


	if err != nil {
		fmt.Printf("error while inializing node: err: %s", err.Error())
		os.Exit(1)
	}

	go node.BatchWrite(ctx, 5 * time.Second)

	if err := node.Listen(ctx); err != nil {
		logger.ErrorContext(ctx, "node error while listening", "err", err.Error())
		os.Exit(1)
	}
}
