# 这是一个什么项目

![What is this project](docker-registry-bs2.png)

本项目的开发内容包括

* 一个完整的 [BS2 Golang SDK](Godeps/_workspace/src/github.com/aclisp/go-bs2/)
* 用 BS2 作后端存储的[驱动插件](registry/storage/driver/bs2/)
* 修改[默认配置](cmd/registry/config-dev.yml)，让其使用 BS2
* 修改 [main.go](cmd/registry/main.go) 静态链接 BS2 驱动

# 如何使用

1. 下载源代码

        git clone <this repo>
        cd <this repo dir>

1. 构建镜像

        docker build -t sigmas/docker-registry-bs2:2.2 .

1. 启动

        docker run -d -p 5000:5000 --name registry sigmas/docker-registry-bs2:2.2

1. 从官方仓库下载某个镜像

        docker pull ubuntu

1. 让这个镜像能够存储到私有仓库

        docker tag ubuntu localhost:5000/myubuntu

1. 上传到私有仓库

        docker push localhost:5000/myubuntu

1. 删除本地镜像

        docker rmi ubuntu
        docker rmi localhost:5000/myubuntu

1. 从私有仓库下载

        docker pull localhost:5000/myubuntu

1. 停止

        docker stop registry && docker rm -v registry


以下是原始 README 内容。
---

# Distribution

The Docker toolset to pack, ship, store, and deliver content.

This repository's main product is the Docker Registry 2.0 implementation
for storing and distributing Docker images. It supersedes the [docker/docker-
registry](https://github.com/docker/docker-registry) project with a new API
design, focused around security and performance.

<img src="https://www.docker.com/sites/default/files/oyster-registry-3.png" width=200px/>

This repository contains the following components:

|**Component**       |Description                                                                                                                                                                                         |
|--------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **registry**       | An implementation of the [Docker Registry HTTP API V2](docs/spec/api.md) for use with docker 1.6+.                                                                                                  |
| **libraries**      | A rich set of libraries for interacting with,distribution components. Please see [godoc](http://godoc.org/github.com/docker/distribution) for details. **Note**: These libraries are **unstable**. |
| **specifications** | _Distribution_ related specifications are available in [docs/spec](docs/spec)                                                                                                                        |
| **documentation**  | Docker's full documentation set is available at [docs.docker.com](http://docs.docker.com). This repository [contains the subset](docs/index.md) related just to the registry.                                                                                                                                          |

### How does this integrate with Docker engine?

This project should provide an implementation to a V2 API for use in the [Docker
core project](https://github.com/docker/docker). The API should be embeddable
and simplify the process of securely pulling and pushing content from `docker`
daemons.

### What are the long term goals of the Distribution project?

The _Distribution_ project has the further long term goal of providing a
secure tool chain for distributing content. The specifications, APIs and tools
should be as useful with Docker as they are without.

Our goal is to design a professional grade and extensible content distribution
system that allow users to:

* Enjoy an efficient, secured and reliable way to store, manage, package and
  exchange content
* Hack/roll their own on top of healthy open-source components
* Implement their own home made solution through good specs, and solid
  extensions mechanism.

## More about Registry 2.0

The new registry implementation provides the following benefits:

- faster push and pull
- new, more efficient implementation
- simplified deployment
- pluggable storage backend
- webhook notifications

For information on upcoming functionality, please see [ROADMAP.md](ROADMAP.md).

### Who needs to deploy a registry?

By default, Docker users pull images from Docker's public registry instance.
[Installing Docker](http://docs.docker.com/installation) gives users this
ability. Users can also push images to a repository on Docker's public registry,
if they have a [Docker Hub](https://hub.docker.com/) account. 

For some users and even companies, this default behavior is sufficient. For
others, it is not. 

For example, users with their own software products may want to maintain a
registry for private, company images. Also, you may wish to deploy your own
image repository for images used to test or in continuous integration. For these
use cases and others, [deploying your own registry instance](docs/deploying.md)
may be the better choice.

### Migration to Registry 2.0

For those who have previously deployed their own registry based on the Registry
1.0 implementation and wish to deploy a Registry 2.0 while retaining images,
data migration is required. A tool to assist with migration efforts has been
created. For more information see [docker/migrator]
(https://github.com/docker/migrator).

## Contribute

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute
issues, fixes, and patches to this project. If you are contributing code, see
the instructions for [building a development environment](docs/building.md).

## Support

If any issues are encountered while using the _Distribution_ project, several
avenues are available for support:

<table>
<tr>
	<th align="left">
	IRC
	</th>
	<td>
	#docker-distribution on FreeNode
	</td>
</tr>
<tr>
	<th align="left">
	Issue Tracker
	</th>
	<td>
	github.com/docker/distribution/issues
	</td>
</tr>
<tr>
	<th align="left">
	Google Groups
	</th>
	<td>
	https://groups.google.com/a/dockerproject.org/forum/#!forum/distribution
	</td>
</tr>
<tr>
	<th align="left">
	Mailing List
	</th>
	<td>
	docker@dockerproject.org
	</td>
</tr>
</table>


## License

This project is distributed under [Apache License, Version 2.0](LICENSE.md).
