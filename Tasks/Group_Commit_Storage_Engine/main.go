package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type LogEntry struct {
	data []byte 
	seq uint64
}

type GroupCommitNode struct {
	Name string
	buffer []LogEntry
	mu sync.Mutex
	cond *sync.Cond

	nextSeq uint64
	flushedSeq uint64

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
	node *GroupCommitNode
}

func (c *Client) HandleConn(ctx context.Context) (error) {
	//defer client connection closing
	defer c.conn.Close()
	buf := make([]byte, 1024)
	for {
		if err := ctx.Err(); err != nil {
			c.logger.ErrorContext(ctx, "context done", "err", err)
			return err
		}
		n, err := c.conn.Read(buf)
		if err != nil {
			c.logger.ErrorContext(ctx, "error while reading conn", "err", err.Error())
			if errors.Is(err, io.EOF){
				return fmt.Errorf("EOF error")
			}
			return err
		}
		
		if n == 0 {
			continue
		}

		if err := c.node.Submit(ctx,buf[:n]); err != nil {
			c.logger.ErrorContext(ctx, "error while node submiting data for a write:", "err", err.Error())
			c.conn.Write([]byte("submit error\n"))
			return err
		}

		//acknowldge that you have received the data
		c.conn.Write([]byte("ACK\n"))
	}

}

func (n *GroupCommitNode) Submit(ctx context.Context, data []byte) error {
	n.mu.Lock()
	n.nextSeq++
	mySeq := n.nextSeq

	//copy the data to avoid mutating the buffer	
	buf := make([]byte, len(data))
	copy(buf, data)
	n.mu.Unlock()

	//add the data to the node buffer
	n.buffer = append(n.buffer, LogEntry{data: buf, seq: mySeq})

	//this function is run by a separate goroutine
	stop := context.AfterFunc(ctx, func() {
		n.cond.L.Lock()
		n.cond.Broadcast()
		n.cond.L.Unlock()
	})
	defer stop()

	n.mu.Lock()
	defer n.mu.Unlock()
	for n.flushedSeq < mySeq {
		if err := ctx.Err(); err != nil {
			n.logger.ErrorContext(ctx, "context done", "err", err.Error())
			return err
		}
		n.cond.Wait()
	}

	return nil
}


func NewNode(name string, proto, addr string, logger *slog.Logger) (*GroupCommitNode, error){
	node := GroupCommitNode{
		Name: name,
		buffer: make([]LogEntry, 0),	
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
	defer ticker.Stop()
	//destination file creation
	fd, err := os.OpenFile("sample.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		n.logger.ErrorContext(ctx, "open file opertion failed", "err", err.Error())
		return fmt.Errorf("error while creating sample file")
	}
	defer fd.Close()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done")
		case <-ticker.C:
			err := n.flushWrite(fd, ctx)
			if err != nil {
				n.logger.ErrorContext(ctx, "error while flush writing to the disk", "err", err.Error())
			}
		}
	}
}

func (n *GroupCommitNode) flushWrite(fd *os.File, ctx context.Context) (error) {
	n.mu.Lock()
	if len(n.buffer) == 0 {
		n.mu.Unlock()
		return nil
	}

	//create a copy of the pending buffer & reset the buffer
	toFlush := n.buffer
	n.buffer = make([]LogEntry, 0)
	//here we get the last sequence number in order to update the flushed sequence
	maxSeq := toFlush[len(toFlush)-1].seq
	n.mu.Unlock()

	//here we are perfoming I/O without holding any lock
	
	//create a new buffer to hold the data...generally i would have flushed the data directly
	//but appending a new line character
	buff := new(bytes.Buffer)
	for _, entry := range toFlush {
		buff.Write(entry.data)
		buff.Write([]byte("\n"))
	}

	//perform the flush sequence
	if _, err := fd.Write(buff.Bytes()); err != nil {
		n.logger.ErrorContext(ctx, "error while writing to disk", "err", err.Error())
		return err
	}


	//update the flushedSeq field to reflect the last point of flushedSeq
	//also broadcast to other listeninig goroutines
	n.mu.Lock()
	n.flushedSeq = maxSeq
	n.cond.Broadcast()
	n.mu.Unlock()

	return nil
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

		client := Client{conn: conn, logger: n.logger, node: n}
		//establish a goroutine to handle client connections
		go func (ctx context.Context, client Client){
			//create a client handler
			timeOutCtx, cancel := context.WithTimeout(ctx, 15 * time.Second)
			defer cancel()
			if err := client.HandleConn(timeOutCtx); err != nil {
				n.logger.ErrorContext(ctx, "error while handling client conn", "err", err.Error())
				return
			}
		}(ctx, client)
	}
}

func main(){

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{}))
	node, err:= NewNode("group_commit_node", "tcp", "0.0.0.0:3224", logger)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM) 
	defer stop()

	if err != nil {
		fmt.Printf("error while inializing node: err: %s", err.Error())
		os.Exit(1)
	}

	go node.BatchWrite(ctx, 5 * time.Second)

	logger.InfoContext(ctx, "node has started listening")
	if err := node.Listen(ctx); err != nil {
		logger.ErrorContext(ctx, "node error while listening", "err", err.Error())
		os.Exit(1)
	}
}
