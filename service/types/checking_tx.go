package types

import (
	"errors"
	"math/big"

	ethCommon "github.com/arcology-network/3rd-party/eth/common"
	ethRlp "github.com/arcology-network/3rd-party/eth/rlp"
	ethTypes "github.com/arcology-network/3rd-party/eth/types"
	"github.com/arcology-network/common-lib/types"
)

type CheckingTx struct {
	Message     types.StandardMessage
	Transaction types.StandardTransaction
}

func (ctx *CheckingTx) UnSign(chainID *big.Int) error {
	otx := ctx.Transaction.Native
	msg, err := otx.AsMessage(ethTypes.NewEIP155Signer(chainID))
	if err != nil {
		return err
	}
	ctx.Message.Native = &msg
	return nil
}

func NewCheckingTxHash(tx []byte, txfrom byte) (*CheckingTx, error) {
	txType := tx[0]
	txReal := tx[1:]
	switch txType {
	case types.TxType_Eth:
		otx := new(ethTypes.Transaction)
		if err := ethRlp.DecodeBytes(txReal, otx); err != nil {
			return nil, err
		}
		txhash := ethCommon.RlpHash(otx)

		checkingTx := CheckingTx{
			Message: types.StandardMessage{
				TxHash:    txhash,
				Source:    txfrom,
				TxRawData: tx,
			},
			Transaction: types.StandardTransaction{
				TxHash:    txhash,
				Native:    otx,
				TxRawData: tx,
				Source:    txfrom,
			},
		}
		return &checkingTx, nil
	}

	return &CheckingTx{}, errors.New("tx type not defined")
}
