package RabbitmqEasy

import (
	"github.com/duolabmeng6/goefun/core"
	"github.com/duolabmeng6/goefun/coreUtil"
	"testing"
)

func TestFuncTimeOut(t *testing.T) {
	go func() {
		时间统计 := coreUtil.New时间统计类()
		i := fib(10)
		core.E调试输出("结果", i, "耗时", 时间统计.E取秒())

		//这里要是超过10秒钟怎么关掉它不让他执行...
		i = fib(1000)
		core.E调试输出("结果", i, "耗时", 时间统计.E取秒())

	}()

	core.E延时(6000000)
}
