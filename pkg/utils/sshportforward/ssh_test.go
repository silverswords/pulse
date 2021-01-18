package sshproxy

func ExampleStartAction() {
	var item = Connect{
		Saddr:       "39.105.141.168:22",
		User:        "root",
		SshAuthType: 2,
		Passwd:      "D:\\34903\\Documents\\abserari.pem",
		Remote:      "172.18.0.2:8080",
		Listen:      "127.0.0.1:8080",
		Connect:     "L",
	}
	StartAction([]Connect{item})
	select {}
}
