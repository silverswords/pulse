package sshproxy

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"golang.org/x/crypto/ssh"
	"io"
	"io/ioutil"
	"log"
	"net"
	"time"
)

var (
	ReTime        int64 = 10 // 连接异常次数后重新打开的等待时间
	HeartBeatTime int64 = 3  // 心跳检测周期
	err           error
)

//{
//"saddr":"120.28.22.113:22", // 远程转发
//"user": "root",
//"type": 2,
//"passwd": ".ssh/auth",
//"Connect": "R",
//"remote": "127.0.0.1:12345",
//"listen": "127.0.0.1:12345"
//},
//{
//"saddr":"120.28.22.113:22", // 本地转发
//"user": "root",
//"type": 1,
//"passwd": "test12345",
//"connect": "L",
//"remote": "120.28.23.113:22",
//"listen": "127.0.0.1:6001",
//"son": [
//{
//"saddr":"127.0.0.1:6001", // 动态转发
//"user": "root",
//"type": 2,
//"passwd": ".ssh/auth_key",
//"connect": "D",
//"listen": "127.0.0.1:60122"
//}
//]
//}
type Connect struct {
	Saddr       string            `json:"saddr"`    // 目标地址
	User        string            `json:"user"`     // 用户
	SshAuthType int               `json:"authType"` // 验证方式 1、密码验证 2、密钥验证
	Passwd      string            `json:"passwd"`   // 密码、密钥路径
	Remote      string            `json:"remote"`   // 远程地址
	Listen      string            `json:"listen"`   // 本地监听地址
	Connect     string            `json:"connect"`  // 连接类型 L 本地转发 R 远程转发 D 动态转发
	Son         []Connect         `json:"son"`      // 子连接
	client      *ssh.Client       // ssh 客户端
	sshConfig   *ssh.ClientConfig // 连接ssh 的配置
}

func StartAction(addrs []Connect) {
	for _, item := range addrs {
		go func(item Connect) {
			item.sshConfig = new(ssh.ClientConfig)
			item.config()
			log.Println("连接目标机器", item.Saddr)
			item.client = new(ssh.Client)
			defer func() {
				if item.client != nil {
					_ = item.client.Close()
				}
			}()
			item.login(false)
			go func() {
				for {
					item.heartbeat()
					time.Sleep(time.Duration(HeartBeatTime) * time.Second)
				}
			}()
			log.Println("服务器", item.Saddr, "连接成功")
			switch item.Connect {
			case "R": // 远程转发
				for {
					item.remoteForward()
					time.Sleep(time.Duration(ReTime) * time.Second)
				}
			case "L": // 本地转发
				for {
					item.socks5ProxyStart(item.localForward)
					time.Sleep(time.Duration(ReTime) * time.Second)
				}

			case "D": // 动态转发
				for {
					item.socks5ProxyStart(item.socks5Proxy)
					time.Sleep(time.Duration(ReTime) * time.Second)
				}

			default:

			}
		}(item)

	}
}

// 登陆远程服务器
func (i *Connect) login(relogin bool) {
	for {
		i.client, err = ssh.Dial("tcp", i.Saddr, i.sshConfig)
		if err == nil {
			break
		}
		if !relogin {
			log.Println("连接远程服务器", i.Saddr, "失败", err.Error())
			panic(err.Error())
		}
		log.Println("尝试重新连接", i.Saddr, err)
		time.Sleep(time.Duration(HeartBeatTime) * time.Second)
	}
}

func (i *Connect) heartbeat() {
	s, err := i.client.NewSession()
	defer func() {
		if s != nil {
			_ = s.Close()
		}
	}()
	if err != nil {
		i.login(true)
		return
	}
	_ = s.Run("")
}

// 配置登陆配置
func (i *Connect) config() {
	var auth ssh.AuthMethod
	if i.SshAuthType == 2 {
		var pKey ssh.Signer
		b, err := ioutil.ReadFile(i.Passwd)
		if err != nil {
			log.Println(i.Saddr, "打开密钥文件失败", err.Error())
			return
		}
		pKey, err = ssh.ParsePrivateKey(b)
		if err != nil {
			log.Println("解析密钥文件失败", err.Error())
			return
		}
		auth = ssh.PublicKeys(pKey)
		i.sshConfig.HostKeyCallback = func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		}
	} else {
		auth = ssh.Password(i.Passwd)
		i.sshConfig.HostKeyCallback = ssh.InsecureIgnoreHostKey()
	}
	i.sshConfig.Auth = []ssh.AuthMethod{
		auth,
	}
	i.sshConfig.User = i.User
}

func (i *Connect) remoteForward() {
	log.Println("远程转发端口监听", i.Remote)
	if i.client == nil {
		i.login(true)
	}
	server, err := i.client.Listen("tcp", i.Remote)
	if err != nil {
		log.Println("建立远程端口失败", i.Remote, err.Error())
		return
	}
	log.Println("远程转发端口监听成功", i.Remote)

	for {
		client, err := server.Accept()
		if err != nil {
			log.Println(i.Remote, "TCP 远程数据接受失败", err.Error())
			return
		}
		go func(conn net.Conn) {
			defer func() {
				if conn != nil {
					_ = conn.Close()
				}
			}()
			server, err := net.Dial("tcp", i.Listen)
			if err != nil {
				log.Println(err.Error())
				return
			}
			go func() {
				_, _ = io.Copy(server, conn)
			}()
			_, _ = io.Copy(conn, server)
		}(client)
	}
}

// 本地转发
func (i *Connect) localForward(conn net.Conn) {
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	if i.client == nil {
		i.login(true)
	}
	server, err := i.client.Dial("tcp", i.Remote)
	defer func() {
		if server != nil {
			_ = server.Close()
		}
	}()
	if err != nil {
		log.Println("本地转发，远程数据传输异常", err.Error())
		return
	}
	go func() {
		_, _ = io.Copy(server, conn)
	}()
	_, _ = io.Copy(conn, server)
}

// 本地转发，和动态转发通用函数
func (i *Connect) socks5ProxyStart(fun func(conn net.Conn)) {
	log.Println("本地端口监听...", i.Listen)
	localServer, err := net.Listen("tcp", i.Listen)
	defer func() {
		if localServer != nil {
			_ = localServer.Close()
		}
	}()
	if err != nil {
		log.Println(i.Saddr, "代理端口监听失败", err.Error())
		return
	}
	log.Println("本地端口监听成功...", i.Listen)
	if len(i.Son) > 0 {
		go func() {
			time.Sleep(1 * time.Second)
			log.Println("开始创建子系统")
			StartAction(i.Son)
		}()
	}

	for {
		client, err := localServer.Accept()
		if err != nil {
			log.Println(i.Saddr, "tcp 数据获取失败", err.Error())
			return
		}
		go fun(client)
	}
}

type sockIP struct {
	A, B, C, D byte
	PORT       uint16
}

func (ip sockIP) toAddr() string {
	return fmt.Sprintf("%d.%d.%d.%d:%d", ip.A, ip.B, ip.C, ip.D, ip.PORT)
}

func (i *Connect) socks5Proxy(conn net.Conn) {
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()
	var b [1024]byte
	_, err := conn.Read(b[:])
	if err != nil {
		if err != io.EOF {
			log.Println(i.Saddr, "目标数据读取失败", err.Error())
		}

		return
	}
	// if len(b[:])>0 {
	// 	Log("数据：",string(b[:]))
	// }
	// log.Printf("% x", b[:n])
	_, _ = conn.Write([]byte{0x05, 0x00})
	n, err := conn.Read(b[:])
	if err != nil {
		if err != io.EOF {
			log.Println("第一次添加数据", i.Saddr, err.Error())
		}

		return
	}
	// log.Printf("% x", b[:n])

	var addr string
	switch b[3] {
	case 0x01:
		sip := sockIP{}
		if err := binary.Read(bytes.NewReader(b[4:n]), binary.BigEndian, &sip); err != nil {
			log.Println(i.Saddr, "请求解析错误", err.Error())
			return
		}
		addr = sip.toAddr()
	case 0x03:
		host := string(b[5 : n-2])
		var port uint16
		err = binary.Read(bytes.NewReader(b[n-2:n]), binary.BigEndian, &port)
		if err != nil {
			log.Println(err)
			return
		}
		addr = fmt.Sprintf("%s:%d", host, port)
	}
	if i.client == nil {
		i.login(true)
	}
	server, err := i.client.Dial("tcp", addr)
	defer func() {
		if server != nil {
			_ = server.Close()
		}
	}()

	if err != nil {
		log.Println("动态转发连接目标失败", err.Error())
		return
	}
	_, _ = conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	go func() {
		_, _ = io.Copy(server, conn)
	}()
	_, _ = io.Copy(conn, server)
}
