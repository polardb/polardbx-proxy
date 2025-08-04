##############################################################
# https://aliyuque.antfin.com/aone/geh0qs/405408294          #
# http://ftp.rpm.org/max-rpm/ch-rpm-inside.html              #
##############################################################
Name: t-polardbx
Version:5.4.20
Release: %(echo $RELEASE)
# if you want use the parameter of rpm_create on build time,
# uncomment below
Summary: alibaba polardbx proxy
Group: alibaba/polardbx
License: Commercial
AutoReqProv: none
Packager: jianghang.loujh@alibaba-inc.com,chenmo.cm@alibaba-inc.com,chenyu.zzy@alibaba-inc.com
%define _prefix /home/admin

BuildArch:noarch

%description
alibaba polardbx

%package proxy
Summary: alibaba polardbx proxy
Group: alibaba/polardbx

%description proxy
alibaba polardbx proxy

%debug_package

%prep

%build
cd $OLDPWD/..
if [ -n "$POLARDBX_SQL_VERSION" ]; then
    mvn clean package -U -DskipTests -Denv=release -Djava_source_version=11 -Djava_target_version=11 -Drevision="%{version}-%{release}-SNAPSHOT" -Dpolardbx.sql.version="$POLARDBX_SQL_VERSION"
else
    mvn clean package -U -DskipTests -Denv=release -Djava_source_version=11 -Djava_target_version=11 -Drevision="%{version}-%{release}-SNAPSHOT"
fi
mkdir -p target/polardbx-proxy/logs
tar zxf target/proxy-server*.tar.gz -C target/polardbx-proxy

%files proxy
%defattr(-,admin,admin)
%{_prefix}/polardbx-proxy
%attr(755,admin,-) %{_prefix}/polardbx-proxy/bin/*.sh
%dir %{_prefix}/polardbx-proxy/logs
%config %{_prefix}/polardbx-proxy/conf/config.properties
%config %{_prefix}/polardbx-proxy/conf/logback.xml

# prepare your files
%install
# OLDPWD is the dir of rpm_create running
# _prefix is an inner var of rpmbuild,
# can set by rpm_create, default is "/home/a"
# _lib is an inner var, maybe "lib" or "lib64" depend on OS

# create dirs
mkdir -p .%{_prefix}
rsync -avz --delete $OLDPWD/../target/polardbx-proxy/ .%{_prefix}/polardbx-proxy/ --exclude=.git

%pre proxy
#define the scripts for pre install
if [ "$1" = "2" ];then
	rm -rf %{_prefix}/polardbx-proxy/lib
fi

%post proxy
#sh %{_prefix}/polardbx-proxy/bin/install.sh
#rm -f %{_prefix}/polardbx-proxy/bin/install.sh

%postun proxy
#define the scripts for pre uninstall
if [ "$1" = "0" ];then
	rm -rf %{_prefix}/polardbx-proxy/bin
	rm -rf %{_prefix}/polardbx-proxy/lib
fi

%define _binaries_in_noarch_packages_terminate_build   0
%undefine _missing_build_ids_terminate_build
