docker build -t sigmas/docker-registry-bs2:2.2 .
docker save sigmas/docker-registry-bs2:2.2 > docker-registry-bs2_2.2.tar
scp -P 32200 docker-registry-bs2_2.2.tar huanghao@jump:~
ssh -p 32200 huanghao@jump scp docker-registry-bs2_2.2.tar 10.25.64.214:~
ssh -p 32200 huanghao@jump ssh 10.25.64.214 sudo docker load < docker-registry-bs2_2.2.tar
