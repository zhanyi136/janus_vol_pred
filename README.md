部署过程


### 安装miniconda并激活环境

进入临时目录
cd /tmp

查看服务器是什么架构
uname -m

下载最新的Miniconda安装脚本
如果是x86_64，输入：
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
添加执行权限
chmod +x Miniconda3-latest-Linux-x86_64.sh
运行安装脚本
bash Miniconda3-latest-Linux-x86_64.sh

如果是aarch64，输入：
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-aarch64.sh
添加执行权限
chmod +x Miniconda3-latest-Linux-aarch64.sh
运行安装脚本
bash Miniconda3-latest-Linux-aarch64.sh

使更改生效（或重新登录服务器）
source ~/.bashrc

验证安装
conda --version

配置conda-forge作为优先通道以获取更多包
conda config --add channels conda-forge

设置通道优先级为灵活模式（减少包冲突）
conda config --set channel_priority flexible

禁用自动激活base环境（避免干扰）
conda config --set auto_activate_base false

加速包下载（并行下载）
conda config --set fetch_threads 5

删除安装脚本
rm /tmp/Miniconda3-latest-Linux-x86_64.sh
或
rm /tmp/Miniconda3-latest-Linux-aarch64.sh

创建名为janus_vol_pred的环境，并安装指定版本3.12.7的Python
conda create -n janus_vol_pred python=3.12.7

激活环境
conda activate janus_vol_pred

### 安装poetry

用官方推荐的安装方式
curl -sSL https://install.python-poetry.org | python3 -

检查Poetry是否在PATH中
which poetry

如果上面的命令没有返回结果，添加Poetry到PATH
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc

检查Poetry版本
poetry --version

在项目目录内创建虚拟环境（推荐）
poetry config virtualenvs.in-project true

安装Poetry shell插件
poetry self add poetry-plugin-shell


### 项目首次创建github项目
创建项目，记得不要勾选​ "Initialize this repository with a README"（因为你有现有文件）

此时本地是有项目文件的， 需推送到github上。
建议选择ssh，而不是http。
推送步骤
```text

cd /home/zhangzhanyi/workspace/janus_vol_pred

# 1. 初始化 git 仓库
git init

# 2. 添加所有文件（.gitignore 会自动过滤不需要的文件）
git add .

# 3. 提交
git commit -m "first commit"

# 4. 设置主分支为 main
git branch -M main

# 5. 关联远程仓库
git remote add origin git@github.com:zhanyi136/janus_vol_pred.git

若用http
git remote add origin https://github.com/zhanyi136/janus_vol_pred.git

# 6. 推送
git push -u origin main

```

不过http每次都要输入密码。
如果想切换成git，只需这样：

git remote set-url origin git@github.com:zhanyi136/janus_vol_pred.git
你可以用 git remote -v 确认一下当前用的是哪个地址。


### 到远程服务器上配置ssh密钥对，并git clone项目


先查看服务器上是否已有 SSH 密钥：
ls ~/.ssh/

如果看到 id_ed25519.pub 或 id_rsa.pub → 说明已有密钥，查看公钥内容：
cat ~/.ssh/id_ed25519.pub
# 或
cat ~/.ssh/id_rsa.pub
然后把这个公钥添加到你的 GitHub 账号就行，不会影响同事原来的密钥

如果 ~/.ssh/ 目录是空的或不存在 → 说明没有密钥，需要生成一个：
ssh-keygen -t ed25519 -C "your_email@example.com"
一路回车即可。

步骤：
生成SSH密钥对
ssh-keygen -t ed25519 -C "你的邮箱地址"
输入下方命令查看公钥
cat ~/.ssh/id_ed25519.pub
添加公钥到Github
把输出的内容复制，添加到你的 GitHub：
Settings → SSH and GPG keys → New SSH key → 粘贴 → 保存
完成后验证：
ssh -T git@github.com
看到 Hi zhanyi136! You've successfully authenticated... 就可以 clone 了。

```bash
[root@ip-172-31-47-106 ~]# ssh -T git@github.com

The authenticity of host 'github.com (20.27.177.113)' can't be established.
ED25519 key fingerprint is SHA256:+DiY3wvvV6TuJJhbpZisF/zLDA0zPMSvHdkr4UvCOqU.
This key is not known by any other names
Are you sure you want to continue connecting (yes/no/[fingerprint])? yes
Warning: Permanently added 'github.com' (ED25519) to the list of known hosts.
Hi zhanyi136! You've successfully authenticated, but GitHub does not provide shell access.
[root@ip-172-31-47-106 ~]# 
```

#### 配置git
登录远程服务器后，直接执行以下命令进行全局配置：
```bash
git config --global user.name "你的用户名"
git config --global user.email "你的邮箱@example.com"
```

### 进入文件夹克隆项目
cd workspace
git clone git@github.com:zhanyi136/janus_vol_pred.git

如果执行
git clone git@github.com:zhanyi136/janus_vol_pred.git
报错
bash: git: command not found
那么说明，目前该远程服务器上没有git

所以要安装git：
步骤：

首先确认你的 Linux 发行版，因为不同发行版的包管理器不同
```bash
# 查看系统详细信息
cat /etc/os-release

# 查看架构确认
uname -m
# 应该显示：aarch64

# 查看发行版名称
cat /etc/*-release
```

拿到返回值后问ai，应该安装什么版本。
像AI告诉我的要安装的版本是：

使用 dnf 安装 Git
```bash
sudo dnf install git -y
```

验证安装
```bash
git --version
```

测试 SSH 连接 GitHub
```bash
ssh -T git@github.com
```

而后就可以执行
```bash
git clone git@github.com:zhanyi136/janus_vol_pred.git
```


### 进入项目后，一键安装依赖

```bash
cd /root/workspace/janus_vol_pred

poetry install

poetry lock
```



## 模型文件管理与传输

### 目录结构

模型文件存放在项目根目录的 `results/` 下：

janus_vol_pred/          ← 项目根
├── janus_vol_pred/      ← Python 代码包
├── results/             ← 模型和结果（gitignore）
│   └── {date}/
│       └── {symbol}/
│           ├── model.txt
│           ├── quantile_transformer.pkl
│           └── feature_cols.json
└── pyproject.toml

### 需要传输的文件

每个 symbol 每天只需传三个文件：

| 文件 | 大小 | 说明 |
|------|------|------|
| `model.txt` | 1~5 MB | LightGBM 树结构（即模型参数） |
| `quantile_transformer.pkl` | < 100 KB | 分位数变换器 |
| `feature_cols.json` | < 1 KB | 特征列名列表 |

### 手动传输命令

单次传输指令
```bash
rsync -avz --mkpath   --include="*/"   --include="model.txt"   --include="quantile_transformer.pkl"   --include="feature_cols.json"   --exclude="*"   /data/sigma/zzy/janus/results/vol_pred_prod/results/2026-03-31/XRPUSDT/   root@54.64.180.220:/root/workspace/janus_vol_pred/results/2026-03-31/XRPUSDT/   -e "ssh -i ~/.ssh/jcd_aws_01/id_ed25519"
```

#### 报错解决
若报错：
```text
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
@         WARNING: UNPROTECTED PRIVATE KEY FILE!          @
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
Permissions 0664 for '/home/zhangzhanyi/.ssh/jcd_aws_01/id_ed25519' are too open.
It is required that your private key files are NOT accessible by others.
This private key will be ignored.
Load key "/home/zhangzhanyi/.ssh/jcd_aws_01/id_ed25519": bad permissions
root@54.64.180.220: Permission denied (publickey,gssapi-keyex,gssapi-with-mic).
rsync: connection unexpectedly closed (0 bytes received so far) [sender]
rsync error: unexplained error (code 255) at io.c(232) [sender=3.2.7]
```

这是
key 文件权限太开放了，修复一下：
chmod 600 ~/.ssh/jcd_aws_01/id_ed25519
然后重新跑 rsync。即可。