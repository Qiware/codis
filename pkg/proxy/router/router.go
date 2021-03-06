// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"strings"
	"sync"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

const MaxSlotNum = models.DEFAULT_SLOT_NUM

type Router struct {
	mu sync.Mutex

	auth string                        // 密码
	pool map[string]*SharedBackendConn // 连接池(主键: "10.110.122.123:6379")

	slots [MaxSlotNum]*Slot // 各slot的信息

	closed bool // 是否关闭
}

// 新建Router对象
func New() *Router {
	return NewWithAuth("")
}

// 新建Router对象
// auth: 密码
func NewWithAuth(auth string) *Router {
	s := &Router{
		auth: auth,
		pool: make(map[string]*SharedBackendConn),
	}
	for i := 0; i < len(s.slots); i++ {
		s.slots[i] = &Slot{id: i}
	}
	return s
}

// 释放Router对象
func (s *Router) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	for i := 0; i < len(s.slots); i++ {
		s.resetSlot(i)
	}
	s.closed = true
	return nil
}

var errClosedRouter = errors.New("use of closed router")

func (s *Router) ResetSlot(i int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosedRouter
	}
	s.resetSlot(i)
	return nil
}

// 填充Router.slots对象
func (s *Router) FillSlot(i int, addr, from string, lock bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosedRouter
	}
	s.fillSlot(i, addr, from, lock)
	return nil
}

// 连接保活处理
func (s *Router) KeepAlive() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosedRouter
	}
	for _, bc := range s.pool {
		bc.KeepAlive() // 给各redis服务发送保活请求
	}
	return nil
}

// 将请求放入指定slot的转发队列
func (s *Router) Dispatch(r *Request) error {
	hkey := getHashKey(r.Resp, r.OpStr)
	slot := s.slots[hashSlot(hkey)]
	return slot.forward(r, hkey)
}

// 获取与addr的连接对象
// addr: "10.110.122.123:6379"
func (s *Router) getBackendConn(addr string) *SharedBackendConn {
	bc := s.pool[addr]
	if bc != nil {
		bc.IncrRefcnt() // 增加引用计数: 复用原有连接
	} else {
		bc = NewSharedBackendConn(addr, s.auth) // 新建与redis的连接(引用计数已+1)
		s.pool[addr] = bc
	}
	return bc
}

// 回收Redis连接
func (s *Router) putBackendConn(bc *SharedBackendConn) {
	if bc != nil && bc.Close() {
		delete(s.pool, bc.Addr())
	}
}

func (s *Router) isValidSlot(i int) bool {
	return i >= 0 && i < len(s.slots)
}

func (s *Router) resetSlot(i int) {
	if !s.isValidSlot(i) {
		return
	}
	slot := s.slots[i]
	slot.blockAndWait()

	s.putBackendConn(slot.backend.bc)
	s.putBackendConn(slot.migrate.bc)
	slot.reset()

	slot.unblock()
}

// 设置slot[i]对象相关信息
func (s *Router) fillSlot(i int, addr, from string, lock bool) {
	if !s.isValidSlot(i) {
		return
	}
	slot := s.slots[i]
	slot.blockAndWait() // 加锁

	s.putBackendConn(slot.backend.bc) // 关闭连接
	s.putBackendConn(slot.migrate.bc) // 关闭连接
	slot.reset()                      // 重置信息

	if len(addr) != 0 {
		xx := strings.Split(addr, ":")
		if len(xx) >= 1 {
			slot.backend.host = []byte(xx[0])
		}
		if len(xx) >= 2 {
			slot.backend.port = []byte(xx[1])
		}
		slot.backend.addr = addr                 // 设置ADDR
		slot.backend.bc = s.getBackendConn(addr) // 获取/新建连接
	}
	if len(from) != 0 {
		slot.migrate.from = from                 // 迁移源地址
		slot.migrate.bc = s.getBackendConn(from) // 获取/新建连接
	}

	if !lock {
		slot.unblock() // 解锁
	}

	if slot.migrate.bc != nil {
		log.Infof("fill slot %04d, backend.addr = %s, migrate.from = %s",
			i, slot.backend.addr, slot.migrate.from)
	} else {
		log.Infof("fill slot %04d, backend.addr = %s",
			i, slot.backend.addr)
	}
}
