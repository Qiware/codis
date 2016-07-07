// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/proxy/router"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/wandoulabs/go-zookeeper/zk"
	topo "github.com/wandoulabs/go-zookeeper/zk"
)

// PROXY服务对象
type Server struct {
	conf   *Config          // 配置信息
	topo   *Topology        // ZK客户端
	info   models.ProxyInfo // PROXY信息
	groups map[int]int      // SLOT -> GROUP映射

	lastActionSeq int // 最近Action序列

	evtbus   chan interface{} // ZK事件队列
	router   *router.Router   // 路由Redis服务的对象
	listener net.Listener     // 帧听端口(19000)

	kill chan interface{} // KILL通道
	wait sync.WaitGroup
	stop sync.Once
}

func New(addr string, debugVarAddr string, conf *Config) *Server {
	log.Infof("create proxy with config: %+v", conf)

	proxyHost := strings.Split(addr, ":")[0]
	debugHost := strings.Split(debugVarAddr, ":")[0]

	hostname, err := os.Hostname()
	if err != nil {
		log.PanicErrorf(err, "get host name failed")
	}
	if proxyHost == "0.0.0.0" || strings.HasPrefix(proxyHost, "127.0.0.") || proxyHost == "" {
		proxyHost = hostname
	}
	if debugHost == "0.0.0.0" || strings.HasPrefix(debugHost, "127.0.0.") || debugHost == "" {
		debugHost = hostname
	}

	s := &Server{conf: conf, lastActionSeq: -1, groups: make(map[int]int)}
	s.topo = NewTopo(conf.productName, conf.zkAddr, conf.fact, conf.provider, conf.zkSessionTimeout)
	s.info.Id = conf.proxyId
	s.info.State = models.PROXY_STATE_OFFLINE
	s.info.Addr = proxyHost + ":" + strings.Split(addr, ":")[1]
	s.info.DebugVarAddr = debugHost + ":" + strings.Split(debugVarAddr, ":")[1]
	s.info.Pid = os.Getpid()
	s.info.StartAt = time.Now().String()
	s.kill = make(chan interface{})

	log.Infof("proxy info = %+v", s.info)

	if l, err := net.Listen(conf.proto, addr); err != nil { // 帧听指定端口
		log.PanicErrorf(err, "open listener failed")
	} else {
		s.listener = l
	}
	s.router = router.NewWithAuth(conf.passwd) // 创建router对象
	s.evtbus = make(chan interface{}, 1024)    // zk事件队列

	s.register() // 向zk服务注册proxy信息

	s.wait.Add(1) // 等待计数增1
	go func() {
		defer s.wait.Done() // 退出时等待计数增1
		s.serve()           // 启动proxy服务
	}()
	return s
}

// 设置PROXY状态为ONLINE
// 描述: 通过向dashboard发送上线请求
func (s *Server) SetMyselfOnline() error {
	log.Info("mark myself online")
	info := models.ProxyInfo{
		Id:    s.conf.proxyId,
		State: models.PROXY_STATE_ONLINE,
	}
	b, _ := json.Marshal(info)
	url := "http://" + s.conf.dashboardAddr + "/api/proxy"
	res, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		return errors.New("response code is not 200")
	}
	return nil
}

// 启动proxy服务
func (s *Server) serve() {
	defer s.close()

	if !s.waitOnline() { // 等待proxy上线成功
		return // 上线失败
	}

	s.rewatchNodes()

	for i := 0; i < router.MaxSlotNum; i++ {
		s.fillSlot(i)
	}
	log.Info("proxy is serving")
	go func() {
		defer s.close()
		s.handleConns()
	}()

	s.loopEvents()
}

// 接收客户端连接请求
func (s *Server) handleConns() {
	ch := make(chan net.Conn, 4096)
	defer close(ch)

	go func() {
		for c := range ch { // 处理新建连接
			x := router.NewSessionSize(c, s.conf.passwd, s.conf.maxBufSize, s.conf.maxTimeout)
			go x.Serve(s.router, s.conf.maxPipeline)
		}
	}()

	for {
		c, err := s.listener.Accept() // 等待连接请求
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.WarnErrorf(err, "[%p] proxy accept new connection failed, get temporary error", s)
				time.Sleep(time.Millisecond * 10)
				continue
			}
			log.WarnErrorf(err, "[%p] proxy accept new connection failed, get non-temporary error, must shutdown", s)
			return
		} else {
			ch <- c // 将新建连接放入队列
		}
	}
}

func (s *Server) Info() models.ProxyInfo {
	return s.info
}

func (s *Server) Join() {
	s.wait.Wait() // 等待服务结束信号
}

func (s *Server) Close() error {
	s.close()
	s.wait.Wait()
	return nil
}

func (s *Server) close() {
	s.stop.Do(func() {
		s.listener.Close()
		if s.router != nil {
			s.router.Close()
		}
		close(s.kill)
	})
}

// 监控Proxy结点信息
func (s *Server) rewatchProxy() {
	_, err := s.topo.WatchNode(
		path.Join(models.GetProxyPath(s.topo.ProductName), s.info.Id), s.evtbus)
	if err != nil {
		log.PanicErrorf(err, "watch node failed")
	}
}

// 监控Actions结点信息
func (s *Server) rewatchNodes() []string {
	nodes, err := s.topo.WatchChildren(
		models.GetWatchActionPath(s.topo.ProductName), s.evtbus)
	if err != nil {
		log.PanicErrorf(err, "watch children failed")
	}
	return nodes
}

/* 向zk服务注册proxy信息 */
func (s *Server) register() {
	// 创建Proxy信息
	if _, err := s.topo.CreateProxyInfo(&s.info); err != nil {
		log.PanicErrorf(err, "create proxy node failed")
	}

	// 创建Fence结点
	if _, err := s.topo.CreateProxyFenceNode(&s.info); err != nil && err != zk.ErrNodeExists {
		log.PanicErrorf(err, "create fence node failed")
	}
	log.Warn("********** Attention **********")
	log.Warn("You should use `kill {pid}` rather than `kill -9 {pid}` to stop me,")
	log.Warn("or the node resisted on zk will not be cleaned when I'm quiting and you must remove it manually")
	log.Warn("*******************************")
}

// 执行下线操作
func (s *Server) markOffline() {
	s.topo.Close(s.info.Id)
	s.info.State = models.PROXY_STATE_MARK_OFFLINE
}

// 等待proxy上线成功
func (s *Server) waitOnline() bool {
	for {
		info, err := s.topo.GetProxyInfo(s.info.Id) // 获取Proxy信息
		if err != nil {
			log.PanicErrorf(err, "get proxy info failed: %s", s.info.Id)
		}

		// 判断proxy状态
		switch info.State {
		case models.PROXY_STATE_MARK_OFFLINE: // 踢下线状态
			log.Infof("mark offline, proxy got offline event: %s", s.info.Id)
			s.markOffline() // 执行下线操作
			return false
		case models.PROXY_STATE_ONLINE: // 上线状态
			s.info.State = info.State
			log.Infof("we are online: %s", s.info.Id)
			s.rewatchProxy()
			return true
		}

		// 当proxy状态为"下线状态"时...
		select {
		case <-s.kill:
			log.Infof("mark offline, proxy is killed: %s", s.info.Id)
			s.markOffline() // 执行下线操作
			return false
		default:
		}
		log.Infof("wait to be online: %s", s.info.Id)
		time.Sleep(3 * time.Second) // 等待3秒
	}
}

func getEventPath(evt interface{}) string {
	return evt.(topo.Event).Path
}

func needResponse(receivers []string, self models.ProxyInfo) bool {
	var info models.ProxyInfo
	for _, v := range receivers {
		err := json.Unmarshal([]byte(v), &info)
		if err != nil {
			if v == self.Id {
				return true
			}
			return false
		}
		if info.Id == self.Id && info.Pid == self.Pid && info.StartAt == self.StartAt {
			return true
		}
	}
	return false
}

// 获取GROUP MASTER的IP地址
func groupMaster(groupInfo models.ServerGroup) string {
	var master string
	for _, server := range groupInfo.Servers {
		if server.Type == models.SERVER_TYPE_MASTER {
			if master != "" {
				log.Panicf("two master not allowed: %+v", groupInfo)
			}
			master = server.Addr
		}
	}
	if master == "" {
		log.Panicf("master not found: %+v", groupInfo)
	}
	return master
}

func (s *Server) resetSlot(i int) {
	s.router.ResetSlot(i)
}

// 填充SLOT[i]信息
func (s *Server) fillSlot(i int) {
	// 获取slot信息&对应group信息
	slotInfo, slotGroup, err := s.topo.GetSlotByIndex(i)
	if err != nil {
		log.PanicErrorf(err, "get slot by index failed", i)
	}

	var from string
	var addr = groupMaster(*slotGroup) // Fromat: "10.110.122.123:6379"
	if slotInfo.State.Status == models.SLOT_STATUS_MIGRATE {
		fromGroup, err := s.topo.GetGroup(slotInfo.State.MigrateStatus.From)
		if err != nil {
			log.PanicErrorf(err, "get migrate from failed")
		}
		from = groupMaster(*fromGroup)
		if from == addr {
			log.Panicf("set slot %04d migrate from %s to %s", i, from, addr)
		}
	}

	s.groups[i] = slotInfo.GroupId
	s.router.FillSlot(i, addr, from,
		slotInfo.State.Status == models.SLOT_STATUS_PRE_MIGRATE)
}

func (s *Server) onSlotRangeChange(param *models.SlotMultiSetParam) {
	log.Infof("slotRangeChange %+v", param)
	for i := param.From; i <= param.To; i++ {
		switch param.Status {
		case models.SLOT_STATUS_OFFLINE:
			s.resetSlot(i)
		case models.SLOT_STATUS_ONLINE:
			s.fillSlot(i)
		default:
			log.Panicf("can not handle status %v", param.Status)
		}
	}
}

func (s *Server) onGroupChange(groupId int) {
	log.Infof("group changed %d", groupId)
	for i, g := range s.groups {
		if g == groupId {
			s.fillSlot(i)
		}
	}
}

func (s *Server) responseAction(seq int64) {
	log.Infof("send response seq = %d", seq)
	err := s.topo.DoResponse(int(seq), &s.info)
	if err != nil {
		log.InfoErrorf(err, "send response seq = %d failed", seq)
	}
}

func (s *Server) getActionObject(seq int, target interface{}) {
	act := &models.Action{Target: target}
	err := s.topo.GetActionWithSeqObject(int64(seq), act)
	if err != nil {
		log.PanicErrorf(err, "get action object failed, seq = %d", seq)
	}
	log.Infof("action %+v", act)
}

func (s *Server) checkAndDoTopoChange(seq int) bool {
	act, err := s.topo.GetActionWithSeq(int64(seq))
	if err != nil { //todo: error is not "not exist"
		log.PanicErrorf(err, "action failed, seq = %d", seq)
	}

	if !needResponse(act.Receivers, s.info) { //no need to response
		return false
	}

	log.Warnf("action %v receivers %v", seq, act.Receivers)

	switch act.Type {
	case models.ACTION_TYPE_SLOT_MIGRATE, models.ACTION_TYPE_SLOT_CHANGED,
		models.ACTION_TYPE_SLOT_PREMIGRATE:
		slot := &models.Slot{}
		s.getActionObject(seq, slot)
		s.fillSlot(slot.Id)
	case models.ACTION_TYPE_SERVER_GROUP_CHANGED:
		serverGroup := &models.ServerGroup{}
		s.getActionObject(seq, serverGroup)
		s.onGroupChange(serverGroup.Id)
	case models.ACTION_TYPE_SERVER_GROUP_REMOVE:
	//do not care
	case models.ACTION_TYPE_MULTI_SLOT_CHANGED:
		param := &models.SlotMultiSetParam{}
		s.getActionObject(seq, param)
		s.onSlotRangeChange(param)
	default:
		log.Panicf("unknown action %+v", act)
	}
	return true
}

// 处理来自zk的行为
func (s *Server) processAction(e interface{}) {
	if strings.Index(getEventPath(e), models.GetProxyPath(s.topo.ProductName)) == 0 {
		info, err := s.topo.GetProxyInfo(s.info.Id) // 获取PROXY信息
		if err != nil {
			log.PanicErrorf(err, "get proxy info failed: %s", s.info.Id)
		}
		switch info.State {
		case models.PROXY_STATE_MARK_OFFLINE: // 下线操作
			log.Infof("mark offline, proxy got offline event: %s", s.info.Id)
			s.markOffline() // 执行下线操作
		case models.PROXY_STATE_ONLINE: // 上线
			s.rewatchProxy() // 关注PROXY结点的消息
		default: // 下线状态(异常)
			log.Panicf("unknown proxy state %v", info)
		}
		return
	}

	//re-watch
	nodes := s.rewatchNodes() // 关注PRODUCT所有子结点消息

	seqs, err := models.ExtraSeqList(nodes)
	if err != nil {
		log.PanicErrorf(err, "get seq list failed")
	}

	if len(seqs) == 0 || !s.topo.IsChildrenChangedEvent(e) {
		return
	}

	//get last pos
	index := -1
	for i, seq := range seqs {
		if s.lastActionSeq < seq {
			index = i
			//break
			//only handle latest action
		}
	}

	if index < 0 {
		return
	}

	actions := seqs[index:]
	for _, seq := range actions {
		exist, err := s.topo.Exist(path.Join(s.topo.GetActionResponsePath(seq), s.info.Id))
		if err != nil {
			log.PanicErrorf(err, "get action failed")
		}
		if exist {
			continue
		}
		if s.checkAndDoTopoChange(seq) {
			s.responseAction(int64(seq))
		}
	}

	s.lastActionSeq = seqs[len(seqs)-1]
}

// 循环等待事件
func (s *Server) loopEvents() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var tick int = 0
	for s.info.State == models.PROXY_STATE_ONLINE {
		select {
		case <-s.kill: // 等待KILL信号
			log.Infof("mark offline, proxy is killed: %s", s.info.Id)
			s.markOffline()
		case e := <-s.evtbus: // 等待来至zk的事件
			evtPath := getEventPath(e)
			log.Infof("got event %s, %v, lastActionSeq %d", s.info.Id, e, s.lastActionSeq)
			if strings.Index(evtPath, models.GetActionResponsePath(s.conf.productName)) == 0 {
				seq, err := strconv.Atoi(path.Base(evtPath))
				if err != nil {
					log.ErrorErrorf(err, "parse action seq failed")
				} else {
					if seq < s.lastActionSeq {
						log.Infof("ignore seq = %d", seq)
						continue
					}
				}
			}
			s.processAction(e) // 处理来自zk的事件
		case <-ticker.C: // 等待超时信号
			if maxTick := s.conf.pingPeriod; maxTick != 0 {
				if tick++; tick >= maxTick {
					s.router.KeepAlive()
					tick = 0
				}
			}
		}
	}
}
