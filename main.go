package main

import (
	"encoding/hex"
	"flag"
	"fmt"
//	"io"
	"log"
	"net"
	"os"
)
var (
     bindAddress = flag.String("bind-address", "127.0.0.1", "bind address")
     localPort = flag.Int("local-port", 6000, "local port")
     remotePort = flag.Int("remote-port", 0, "remote port")
     remoteHost = flag.String("remote-host", "", "remote host")
     bufferSize = flag.Int("buffer-size", 512, "buffer size")
     displayLogs = flag.Bool("log", false, "log")
     logFormat = flag.String("log-format", "raw", "log format (values: raw, hex)")
)
func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options...]\n", os.Args[0])
		fmt.Fprint(os.Stderr, "\n")
		fmt.Fprint(os.Stderr, "Options:\n")
		fmt.Fprint(os.Stderr, "\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *bindAddress, *localPort))
	if err != nil {
		log.Fatal("Failed to listen on local port: ", err)
	}
	log.Printf("Proxy listening on %s:%d, forwarding to %s:%d", *bindAddress, *localPort, *remoteHost, *remotePort)

	for {
		localConn, err := listener.Accept()
		if err != nil {
			log.Print("Accept error: ", err)
			continue
		}

		if *displayLogs {
			log.Printf("Accepted connection from: %s", localConn.RemoteAddr())
	        }
		go handleConn(localConn)
	}
}

func handleConn(localConn net.Conn) {
	defer localConn.Close()

	remoteConn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", *remoteHost, *remotePort))
	if err != nil {
		log.Printf("Failed to connect to remote host: %v", err)
		return
	}
	defer remoteConn.Close()
	if *displayLogs {
		log.Printf("Connected to remote %s:%d", *remoteHost, *remotePort)
	}

	errChan := make(chan error, 2) //bidirectional
	go proxyLoop(localConn, remoteConn, errChan, true)
	go proxyLoop(remoteConn, localConn, errChan, false)
	<-errChan
}

func proxyLoop(from net.Conn, to net.Conn, errChan chan error, localToRemote bool) {
	buffer := make([]byte, *bufferSize)
	for {
		n, err := from.Read(buffer)
		if n > 0 {
			if *displayLogs {
				var logPrefix string
				if localToRemote {
					logPrefix = ">>> local to remote"
				} else {
					logPrefix = "<<< remote to local"
				}
				if *logFormat == "hex" {
					log.Print(logPrefix, "\n", hex.Dump(buffer[:n]))
				} else {
					log.Printf("%s\n%s", logPrefix, string(buffer[:n]))
				}
			}

			_, err2 := to.Write(buffer[:n])
			if err2 != nil {
				errChan <- err2
				return
			}
		}
		if err != nil {
			errChan <- err
			return
		}
	}
}
