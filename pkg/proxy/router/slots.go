// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"fmt"
	"sync"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

// SLOT对象
type Slot struct {
	id int // SLOT编号

	backend struct { // Redis服务: SLOT存储到目标Redis服务
		addr string             // Redis地址: "10.110.122.123:6379"
		host []byte             // Redis地址: "10.110.122.123"
		port []byte             // Redis端口: "6379"
		bc   *SharedBackendConn // Redis连接对象
	}
	migrate struct { // Redis服务: 需要将此Redis服务的数据迁移到目标Redis服务
		from string             // Redis地址: "10.110.122.123:6379"
		bc   *SharedBackendConn // Redis连接对象
	}

	wait sync.WaitGroup
	lock struct {
		hold bool
		sync.RWMutex
	}
}

func (s *Slot) blockAndWait() {
	if !s.lock.hold {
		s.lock.hold = true
		s.lock.Lock() // 加锁
	}
	s.wait.Wait() // 等待信号
}

func (s *Slot) unblock() {
	if !s.lock.hold {
		return
	}
	s.lock.hold = false
	s.lock.Unlock() // 解锁
}

func (s *Slot) reset() {
	s.backend.addr = ""
	s.backend.host = nil
	s.backend.port = nil
	s.backend.bc = nil
	s.migrate.from = ""
	s.migrate.bc = nil
}

// 转发请求：将请求放入bc发送队列
func (s *Slot) forward(r *Request, key []byte) error {
	s.lock.RLock()
	bc, err := s.prepare(r, key) // 预处理
	s.lock.RUnlock()
	if err != nil {
		return err
	} else {
		bc.PushBack(r) // 将请求放入转发队列
		return nil
	}
}

var ErrSlotIsNotReady = errors.New("slot is not ready, may be offline")

// 预处理：将请求发送给目标Redis服务之前的处理
func (s *Slot) prepare(r *Request, key []byte) (*SharedBackendConn, error) {
	if s.backend.bc == nil {
		log.Infof("slot-%04d is not ready: key = %s", s.id, key)
		return nil, ErrSlotIsNotReady
	}
	if err := s.slotsmgrt(r, key); err != nil { // 发起迁移请求
		log.Warnf("slot-%04d migrate from = %s to %s failed: key = %s, error = %s",
			s.id, s.migrate.from, s.backend.addr, key, err)
		return nil, err
	} else {
		r.slot = &s.wait
		r.slot.Add(1)
		return s.backend.bc, nil
	}
}

// 发起迁移请求
// 当迁移过程中发生数据访问时, Proxy会发送”slotsmgrttagone”迁移命令给Redis, 强制
// 将客户端要访问的Key立刻迁移, 然后再处理客户端的请求.
func (s *Slot) slotsmgrt(r *Request, key []byte) error {
	if len(key) == 0 || s.migrate.bc == nil {
		return nil
	}
	m := &Request{
		Resp: redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte("SLOTSMGRTTAGONE")), // 立即迁移请求
			redis.NewBulkBytes(s.backend.host),            // 目标IP
			redis.NewBulkBytes(s.backend.port),            // 目标PORT
			redis.NewBulkBytes([]byte("3000")),            // 超时时间
			redis.NewBulkBytes(key),                       // 迁移KEY
		}),
		Wait: &sync.WaitGroup{},
	}
	s.migrate.bc.PushBack(m)

	m.Wait.Wait()

	resp, err := m.Response.Resp, m.Response.Err
	if err != nil {
		return err
	}
	if resp == nil {
		return ErrRespIsRequired
	}
	if resp.IsError() {
		return errors.New(fmt.Sprintf("error resp: %s", resp.Value))
	}
	if resp.IsInt() {
		log.Debugf("slot-%04d migrate from %s to %s: key = %s, resp = %s",
			s.id, s.migrate.from, s.backend.addr, key, resp.Value)
		return nil
	} else {
		return errors.New(fmt.Sprintf("error resp: should be integer, but got %s", resp.Type))
	}
}
