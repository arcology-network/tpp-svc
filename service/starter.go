package service

import (
	tmCommon "github.com/arcology-network/3rd-party/tm/common"
	"github.com/arcology-network/component-lib/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var StartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start prepare process service Daemon",
	RunE:  startCmd,
}

func init() {
	flags := StartCmd.Flags()

	flags.String("mqaddr", "localhost:9092", "host:port of kafka")
	flags.String("mqaddr2", "localhost:9092", "host:port of kafka")

	flags.String("block-txs", "block-txs", "topic for received txs from block")
	flags.String("checked-txs", "checked-txs", "topic for receive checked txs")

	flags.String("chkd-message", "chkd-message", "topic for send message")

	flags.Int("concurrency", 4, "num of threads")

	flags.String("log", "log", "topic for send log")

	flags.String("msgexch", "msgexch", "topic of received or send msg exchange")

	flags.String("logcfg", "./log.toml", "log conf path")

	flags.Int("nidx", 0, "node index in cluster")
	flags.String("nname", "node1", "node name in cluster")

	flags.String("zkUrl", "127.0.0.1:2181", "url of zookeeper")
	flags.String("localIp", "127.0.0.1", "local ip of server")

}

func startCmd(cmd *cobra.Command, args []string) error {
	log.InitLog("ppt.log", viper.GetString("logcfg"), "ppt", viper.GetString("nname"), viper.GetInt("nidx"))

	en := NewConfig()
	en.Start()

	// Wait forever
	tmCommon.TrapSignal(func() {
		// Cleanup
		en.Stop()
	})

	return nil
}
