package workers

import (
	"fmt"

	"github.com/arcology-network/common-lib/common"
	"github.com/arcology-network/component-lib/actor"
	"github.com/arcology-network/component-lib/log"
	pptTypes "github.com/arcology-network/ppt-svc/service/types"
	"go.uber.org/zap"
)

type TxReceiver struct {
	actor.WorkerThread
}

//return a Subscriber struct
func NewTxReceiver(concurrency int, groupid string) *TxReceiver {
	receiver := TxReceiver{}
	receiver.Set(concurrency, groupid)

	return &receiver
}

func (r *TxReceiver) OnStart() {
}

func (r *TxReceiver) OnMessageArrived(msgs []*actor.Message) error {
	for _, v := range msgs {
		switch v.Name {
		case actor.MsgCheckedTxs:
			data := v.Data.([][]byte)
			r.parallelSendTxs(data)
		}
	}

	return nil
}

func (r *TxReceiver) parallelSendTxs(rawtxs [][]byte) {
	txLen := len(rawtxs)
	checks := make([]*pptTypes.CheckingTx, txLen)
	common.ParallelWorker(txLen, r.Concurrency, r.checkingTxHashWorker, rawtxs, &checks)

	r.AddLog(log.LogLevel_Debug, "parallelSendTxs completed <<<<<<<<<<", zap.Int("txLen", len(checks)))
	r.MsgBroker.Send(actor.MsgCheckingTxs, checks)
}
func (r *TxReceiver) checkingTxHashWorker(start, end, idx int, args ...interface{}) {
	txs := args[0].([]interface{})[0].([][]byte)
	checks := args[0].([]interface{})[1].(*[]*pptTypes.CheckingTx)

	for i, tx := range txs[start:end] {
		checkingTx, err := pptTypes.NewCheckingTxHash(tx[1:], tx[0])
		if err != nil {
			r.AddLog(log.LogLevel_Error, "received block tx ", zap.Int("idx", i+start), zap.String("err", err.Error()), zap.String("tx", fmt.Sprintf("%x", tx)), zap.String("from", fmt.Sprintf("%x", tx[0])))
			continue
		}
		(*checks)[i+start] = checkingTx
	}
}
