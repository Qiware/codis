// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"fmt"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

// 与redis连接对象
type BackendConn struct {
	addr string // 地址: 10.110.122.123:6379
	auth string // 密码
	stop sync.Once

	input chan *Request // 此队列的数据是客户端发起的Redis操作请求, 将会发送到Redis服务
}

// 与Redis建立连接
func NewBackendConn(addr, auth string) *BackendConn {
	bc := &BackendConn{
		addr: addr, auth: auth,
		input: make(chan *Request, 1024),
	}
	go bc.Run() // 运行
	return bc
}

// 与Redis服务进行通信
func (bc *BackendConn) Run() {
	log.Infof("backend conn [%p] to %s, start service", bc, bc.addr)
	for k := 0; ; k++ {
		err := bc.loopWriter()
		if err == nil {
			break
		} else {
			for i := len(bc.input); i != 0; i-- {
				r := <-bc.input             // 从input队列获取应答
				bc.setResponse(r, nil, err) // 设置应答信息
			}
		}
		log.WarnErrorf(err, "backend conn [%p] to %s, restart [%d]", bc, bc.addr, k)
		time.Sleep(time.Millisecond * 50)
	}
	log.Infof("backend conn [%p] to %s, stop and exit", bc, bc.addr)
}

func (bc *BackendConn) Addr() string {
	return bc.addr // 返回Redis服务端地址: "10.110.122.123:19000"
}

func (bc *BackendConn) Close() {
	bc.stop.Do(func() {
		close(bc.input)
	})
}

func (bc *BackendConn) PushBack(r *Request) {
	if r.Wait != nil {
		r.Wait.Add(1) // 请求等待计数+1
	}
	bc.input <- r
}

// 给redis服务发送保活请求
func (bc *BackendConn) KeepAlive() bool {
	if len(bc.input) != 0 {
		return false
	}
	// 创建保活请求
	r := &Request{
		Resp: redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte("PING")),
		}),
	}

	select {
	case bc.input <- r: // 将请求放入INPUT队列
		return true
	default:
		return false
	}
}

var ErrFailedRequest = errors.New("discard failed request")

func (bc *BackendConn) loopWriter() error {
	r, ok := <-bc.input // 从input队列获取Request对象
	if ok {
		// c: 与redis的连接
		// tasks: 任务通道
		c, tasks, err := bc.newBackendReader() // 从服务端读取应答
		if err != nil {
			return bc.setResponse(r, nil, err)
		}
		defer close(tasks)

		p := &FlushPolicy{
			Encoder:     c.Writer,
			MaxBuffered: 64,
			MaxInterval: 300,
		}
		// 循环: 发送应答至客户端
		for ok {
			var flush = len(bc.input) == 0
			if bc.canForward(r) {
				if err := p.Encode(r.Resp, flush); err != nil { // 发送应答至客户端
					return bc.setResponse(r, nil, err)
				}
				tasks <- r
			} else {
				if err := p.Flush(flush); err != nil {
					return bc.setResponse(r, nil, err)
				}
				bc.setResponse(r, nil, ErrFailedRequest)
			}

			r, ok = <-bc.input // 从input队列获取Request对象

		}
	}
	return nil
}

// 接收来自redis服务端的数据
func (bc *BackendConn) newBackendReader() (*redis.Conn, chan<- *Request, error) {
	/* 建立与redis服务端的连接 */
	c, err := redis.DialTimeout(bc.addr, 1024*512, time.Second)
	if err != nil {
		return nil, nil, err
	}
	c.ReaderTimeout = time.Minute
	c.WriterTimeout = time.Minute

	if err := bc.verifyAuth(c); err != nil {
		c.Close()
		return nil, nil, err
	}

	tasks := make(chan *Request, 4096)
	go func() {
		defer c.Close()
		for r := range tasks {
			resp, err := c.Reader.Decode() // 发送tasks中的请求
			bc.setResponse(r, resp, err)
			if err != nil {
				// close tcp to tell writer we are failed and should quit
				c.Close()
			}
		}
	}()
	return c, tasks, nil
}

// 发生鉴权数据至Redis服务, 并判断是否鉴权成功
func (bc *BackendConn) verifyAuth(c *redis.Conn) error {
	if bc.auth == "" {
		return nil
	}
	resp := redis.NewArray([]*redis.Resp{
		redis.NewBulkBytes([]byte("AUTH")),
		redis.NewBulkBytes([]byte(bc.auth)),
	})

	if err := c.Writer.Encode(resp, true); err != nil {
		return err
	}

	resp, err := c.Reader.Decode()
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.New(fmt.Sprintf("error resp: nil response"))
	}
	if resp.IsError() {
		return errors.New(fmt.Sprintf("error resp: %s", resp.Value))
	}
	if resp.IsString() {
		return nil
	} else {
		return errors.New(fmt.Sprintf("error resp: should be string, but got %s", resp.Type))
	}
}

// 判断请求是否异常
func (bc *BackendConn) canForward(r *Request) bool {
	if r.Failed != nil && r.Failed.Get() {
		return false
	} else {
		return true
	}
}

func (bc *BackendConn) setResponse(r *Request, resp *redis.Resp, err error) error {
	r.Response.Resp, r.Response.Err = resp, err
	if err != nil && r.Failed != nil {
		r.Failed.Set(true)
	}
	if r.Wait != nil {
		r.Wait.Done() // 等待计数减1
	}
	if r.slot != nil {
		r.slot.Done() // 等待计数减1
	}
	return err
}

type SharedBackendConn struct {
	*BackendConn
	mu sync.Mutex

	refcnt int
}

// 新建与Redis的连接
// addr: 其格式为"10.110.122.123:6379"
// auth: Redis密码
func NewSharedBackendConn(addr, auth string) *SharedBackendConn {
	return &SharedBackendConn{BackendConn: NewBackendConn(addr, auth), refcnt: 1}
}

func (s *SharedBackendConn) Close() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refcnt <= 0 {
		log.Panicf("shared backend conn has been closed, close too many times")
	}
	if s.refcnt == 1 {
		s.BackendConn.Close()
	}
	s.refcnt--
	return s.refcnt == 0
}

// 增加引用计数
func (s *SharedBackendConn) IncrRefcnt() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refcnt == 0 {
		log.Panicf("shared backend conn has been closed")
	}
	s.refcnt++
}

type FlushPolicy struct {
	*redis.Encoder // 发送接口

	MaxBuffered int
	MaxInterval int64

	nbuffered int
	lastflush int64
}

func (p *FlushPolicy) needFlush() bool {
	if p.nbuffered != 0 {
		if p.nbuffered > p.MaxBuffered {
			return true
		}
		if microseconds()-p.lastflush > p.MaxInterval {
			return true
		}
	}
	return false
}

func (p *FlushPolicy) Flush(force bool) error {
	if force || p.needFlush() {
		if err := p.Encoder.Flush(); err != nil {
			return err
		}
		p.nbuffered = 0
		p.lastflush = microseconds()
	}
	return nil
}

func (p *FlushPolicy) Encode(resp *redis.Resp, force bool) error {
	if err := p.Encoder.Encode(resp, false); err != nil {
		return err
	} else {
		p.nbuffered++
		return p.Flush(force)
	}
}
