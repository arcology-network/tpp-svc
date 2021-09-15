package service

import (
	"net/http"

	"github.com/arcology-network/component-lib/actor"
	"github.com/arcology-network/component-lib/streamer"
	"github.com/arcology-network/ppt-svc/service/workers"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"

	"github.com/arcology-network/component-lib/kafka"
)

type Config struct {
	concurrency int
	groupid     string
}

//return a Subscriber struct
func NewConfig() *Config {
	return &Config{
		concurrency: viper.GetInt("concurrency"),
		groupid:     "ppt",
	}
}

func (cfg *Config) Start() {

	http.Handle("/streamer", promhttp.Handler())
	go http.ListenAndServe(":19007", nil)

	broker := streamer.NewStatefulStreamer()
	//00 initializer
	initializer := actor.NewActor(
		"initializer",
		broker,
		[]string{actor.MsgStarting},
		[]string{
			actor.MsgStartSub,
		},
		[]int{1},
		workers.NewInitializer(cfg.concurrency, cfg.groupid),
	)
	initializer.Connect(streamer.NewDisjunctions(initializer, 1))

	//01 kafkaDownloader
	receiveMseeages := []string{
		actor.MsgCheckedTxs,
	}

	receiveTopics := []string{
		viper.GetString("checked-txs"),
	}
	kafkaDownloader := actor.NewActor(
		"kafkaDownloader",
		broker,
		[]string{actor.MsgStartSub},
		receiveMseeages,
		[]int{100},
		kafka.NewKafkaDownloader(cfg.concurrency, cfg.groupid, receiveTopics, receiveMseeages, viper.GetString("mqaddr2")),
	)
	kafkaDownloader.Connect(streamer.NewDisjunctions(kafkaDownloader, 1000))

	//02 txReceiver
	txReceiver := actor.NewActor(
		"txReceiver",
		broker,
		[]string{
			actor.MsgCheckedTxs,
		},
		[]string{
			actor.MsgCheckingTxs,
		},
		[]int{10},
		workers.NewTxReceiver(cfg.concurrency, cfg.groupid),
	)
	txReceiver.Connect(streamer.NewDisjunctions(txReceiver, 1000))

	//03 unsigner
	unsigner := actor.NewActor(
		"unsigner",
		broker,
		[]string{
			actor.MsgCheckingTxs,
		},
		[]string{
			actor.MsgMessager,
		},
		[]int{1},
		workers.NewTxUnsigner(cfg.concurrency, cfg.groupid),
	)
	unsigner.Connect(streamer.NewDisjunctions(unsigner, 1))

	relations := map[string]string{}

	relations[actor.MsgMessager] = viper.GetString("chkd-message")
	//04 kafkaUploader
	kafkaUploader := actor.NewActor(
		"kafkaUploader",
		broker,
		[]string{
			actor.MsgMessager,
			//actor.MsgRawtx,
		},
		[]string{},
		[]int{1},
		kafka.NewKafkaUploader(cfg.concurrency, cfg.groupid, relations, viper.GetString("mqaddr2")),
	)
	kafkaUploader.Connect(streamer.NewDisjunctions(kafkaUploader, 1))

	//starter
	selfStarter := streamer.NewDefaultProducer("selfStarter", []string{actor.MsgStarting}, []int{1})
	broker.RegisterProducer(selfStarter)
	broker.Serve()

	//start signel
	streamerStarting := actor.Message{
		Name:   actor.MsgStarting,
		Height: 0,
		Round:  0,
		Data:   "start",
	}
	broker.Send(actor.MsgStarting, &streamerStarting)
}

func (cfg *Config) Stop() {

}
