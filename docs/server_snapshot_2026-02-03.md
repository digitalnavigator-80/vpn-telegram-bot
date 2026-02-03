# Server snapshot (2026-02-03T09:48:25+00:00)

## Host
- Hostname: 281700.fornex.cloud
- OS: Ubuntu 24.04.3 LTS
- Kernel: 6.8.0-90-generic

## Project
- Path: /opt/vpn-telegram-bot
- Git branch: main
- Git commit: 36e5c1b
- Remotes:
origin	git@github.com:digitalnavigator-80/vpn-telegram-bot.git (fetch)
origin	git@github.com:digitalnavigator-80/vpn-telegram-bot.git (push)

## Docker
Docker version 29.2.0, build 0b9d198
Docker Compose version v5.0.2

## Docker Compose status
NAME               IMAGE                      COMMAND           SERVICE   CREATED       STATUS       PORTS
vpn-telegram-bot   vpn-telegram-bot-vpn-bot   "python bot.py"   vpn-bot   3 hours ago   Up 3 hours   

## Key .env vars (values REDACTED)
2:YOOKASSA_SHOP_ID=***REDACTED***
3:YOOKASSA_SECRET_KEY=***REDACTED***
4:BOT_TOKEN=***REDACTED***
5:MARZBAN_TOKEN=***REDACTED***
8:PAYMENT_RETURN_URL=***REDACTED***
11:PAYMENT_WEBHOOK_URL=***REDACTED***
13:PUBLIC_BASE_URL=***REDACTED***
15:YOOKASSA_WEBHOOK_SECRET=***REDACTED***

## Listening ports (top)
Netid State  Recv-Q Send-Q Local Address:Port  Peer Address:PortProcess                                                      
tcp   LISTEN 0      4096       127.0.0.1:33595      0.0.0.0:*    users:(("xray",pid=136346,fd=6))                            
tcp   LISTEN 0      128        127.0.0.1:6010       0.0.0.0:*    users:(("sshd",pid=412243,fd=7))                            
tcp   LISTEN 0      2048       127.0.0.1:8000       0.0.0.0:*    users:(("python",pid=136146,fd=9))                          
tcp   LISTEN 0      511          0.0.0.0:443        0.0.0.0:*    users:(("nginx",pid=423297,fd=11),("nginx",pid=20492,fd=11))
tcp   LISTEN 0      128          0.0.0.0:8080       0.0.0.0:*    users:(("python",pid=376217,fd=6))                          
tcp   LISTEN 0      4096         0.0.0.0:22         0.0.0.0:*    users:(("sshd",pid=13288,fd=3),("systemd",pid=1,fd=181))    
tcp   LISTEN 0      511          0.0.0.0:80         0.0.0.0:*    users:(("nginx",pid=423297,fd=5),("nginx",pid=20492,fd=5))  
tcp   LISTEN 0      128            [::1]:6010          [::]:*    users:(("sshd",pid=412243,fd=5))                            
tcp   LISTEN 0      4096            [::]:22            [::]:*    users:(("sshd",pid=13288,fd=4),("systemd",pid=1,fd=183))    
tcp   LISTEN 0      511             [::]:80            [::]:*    users:(("nginx",pid=423297,fd=6),("nginx",pid=20492,fd=6))  
tcp   LISTEN 0      4096               *:8443             *:*    users:(("xray",pid=136346,fd=3))                            
