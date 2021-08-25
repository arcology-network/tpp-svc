package workers

import (
	"math"
	"math/big"
	"sync"

	ethParams "github.com/arcology/3rd-party/eth/params"
	"github.com/arcology/common-lib/types"
	"github.com/arcology/component-lib/actor"
	"github.com/arcology/component-lib/log"
	ppttypes "github.com/arcology/ppt-svc/service/types"
	"go.uber.org/zap"
)

type TxUnsigner struct {
	actor.WorkerThread
	chainID *big.Int
}

//return a Subscriber struct
func NewTxUnsigner(concurrency int, groupid string) *TxUnsigner {
	unsigner := TxUnsigner{}
	unsigner.Set(concurrency, groupid)
	return &unsigner
}

func (c *TxUnsigner) OnStart() {
	c.chainID = ethParams.MainnetChainConfig.ChainID

}

func (c *TxUnsigner) OnMessageArrived(msgs []*actor.Message) error {
	for _, v := range msgs {
		switch v.Name {
		case actor.MsgCheckingTxs:
			checkingTxs := v.Data.([]*ppttypes.CheckingTx)
			filteredTxs := make([]*ppttypes.CheckingTx, 0, len(checkingTxs))
			for i := range checkingTxs {

				if checkingTxs[i] == nil {
					c.AddLog(log.LogLevel_Error, "checkingTxs is nil >>>>>>>>>>>>>>>>>>", zap.Int("idx", i))
					continue
				}
				filteredTxs = append(filteredTxs, checkingTxs[i])
			}

			messages := c.unSignTxs(filteredTxs)
			c.MsgBroker.Send(actor.MsgMessager, messages)
		}
	}
	return nil
}

func (c *TxUnsigner) unSignTxs(ctxs []*ppttypes.CheckingTx) []*types.StandardMessage {
	txLen := len(ctxs)
	threads := c.Concurrency
	var step = int(math.Max(float64(txLen/threads), float64(txLen%threads)))
	wg := sync.WaitGroup{}
	c.AddLog(log.LogLevel_Debug, "start decodeing txs>>>>>>>>>>>>>>>>>>", zap.Int("txLen", txLen))
	messages := make([]*types.StandardMessage, len(ctxs))
	for counter := 0; counter <= threads; counter++ {
		begin := counter * step
		end := int(math.Min(float64(begin+step), float64(txLen)))
		wg.Add(1)
		go func(begin int, end int, id int) {
			for i := begin; i < end; i++ {
				ctxs[i].UnSign(c.chainID)
				messages[i] = &ctxs[i].Message
			}
			wg.Done()
		}(begin, end, counter)
		if txLen == end {
			break
		}
	}
	wg.Wait()
	c.AddLog(log.LogLevel_Debug, "decodeing txs completed <<<<<<<<<<<<<<<<<<<<<<<<<<<", zap.Int("txLen", txLen))
	return messages
}
