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

