// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"sync"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/atomic2"
)

type Dispatcher interface {
	Dispatch(r *Request) error
}

// 请求信息
type Request struct {
	OpStr string
	Start int64

	Resp *redis.Resp // 请求数据

	Coalesce func() error // 合并应答回调
	Response struct {     // 应答内容
		Resp *redis.Resp // 应答数据
		Err  error       // 错误信息
	}

	Wait *sync.WaitGroup // 等待应答
	slot *sync.WaitGroup // 标识当前请求等待哪个SLOT的信号

	Failed *atomic2.Bool // 是否异常
}
