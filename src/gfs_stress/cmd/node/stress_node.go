package main

import (
	"bufio"
	"flag"
	"fmt"
	"gfs_stress"
	"log"
	"os"
	"strings"
)

func readConfigFile(path string) gfs_stress.Config {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalln("cannot read config file:", err)
	}
	defer f.Close()
	r := bufio.NewReader(f)
	conf := gfs_stress.Config{}
	for {
		s, err := r.ReadString('\n')
		if err != nil {
			break
		}
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		splited := strings.Split(s, "=")
		if len(splited) != 2 {
			log.Fatalln("Line ", s, ": should have exactly one '='")
		}
		k, v := strings.TrimSpace(splited[0]), strings.TrimSpace(splited[1])
		switch k {
		case "id":
			conf.ID = v
		case "role":
			conf.Role = v
		case "listen":
			conf.Listen = v
		case "master":
			conf.Master = v
		case "center":
			conf.Center = v
		case "eth":
			conf.NetInterface = v
		default:
			log.Fatalln("Line ", s, ": Unknown Key")
		}
	}
	return conf
}

func main() {
	gfs_stress.WritePID()
	conf := flag.String("conf", "", "path to configuration file")
	id := flag.String("id", "", "server ID")
	role := flag.String("role", "", "master/chunkserver")
	listen := flag.String("listen", "", "listen address")
	master := flag.String("master", "", "master address")
	center := flag.String("center", "", "stress test center address")
	eth := flag.String("eth", "", "network interface name")
	flag.Parse()

	var cfg gfs_stress.Config
	if *conf != "" {
		cfg = readConfigFile(*conf)
	}
	if *id != "" {
		cfg.ID = *id
	}
	if *role != "" {
		cfg.Role = *role
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	if *master != "" {
		cfg.Master = *master
	}
	if *center != "" {
		cfg.Center = *center
	}
	if *eth != "" {
		cfg.NetInterface = *eth
	}

	if cfg.ID == "" ||
		cfg.Role == "" ||
		cfg.Center == "" ||
		cfg.Listen == "" ||
		cfg.NetInterface == "" ||
		(cfg.Role != "master" && cfg.Role != "chunkserver") ||
		(cfg.Role == "master" && cfg.Master != "") ||
		(cfg.Role == "chunkserver" && cfg.Master == "") {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	gfs_stress.Run(cfg)
}
