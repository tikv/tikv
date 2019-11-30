%global __os_install_post /usr/lib/rpm/brp-compress %{nil}
%global __spec_install_post %{nil}
%global debug_package %{nil}

Name:           tikv
Version:        4.0.0.alpha
Release:        1%{?dist}
Summary:        Distributed transactional key value database powered by Rust and Raft

License:        Apache License 2.0
URL:            https://github.com/tikv/tikv

Source0:        %{name}-package.tar.gz

Requires:       systemd
Requires(pre):  shadow-utils
Requires(post): systemd

%description
A distributed transactional key-value database.

%prep
%setup -q
%build
%install
ls -lah
%{__mkdir} -p $RPM_BUILD_ROOT%{_bindir}
%{__mkdir} -p $RPM_BUILD_ROOT%{_sharedstatedir}/tikv
%{__mkdir} -p $RPM_BUILD_ROOT%{_localstatedir}/log/tikv
%{__install} -D -m 0755 bin/tikv-server $RPM_BUILD_ROOT%{_bindir}/tikv-server
%{__install} -D -m 0755 bin/tikv-ctl $RPM_BUILD_ROOT%{_bindir}/tikv-ctl
%{__install} -D -m 0644 etc/config-template.toml $RPM_BUILD_ROOT%{_sysconfdir}/tikv/tikv.toml
%{__install} -D -m 0644 etc/tikv.service $RPM_BUILD_ROOT%{_unitdir}/tikv.service
%{__install} -D -m 0644 etc/tikv.sysconfig $RPM_BUILD_ROOT%{_sysconfdir}/sysconfig/tikv

%pre
getent passwd tikv >/dev/null || useradd -r -s /sbin/nologin -d /var/lib/tikv tikv
exit 0

%post
%systemd_post tikv.service

%preun
%systemd_preun tikv.service

%files
%{_bindir}/tikv-server
%{_bindir}/tikv-ctl
%{_unitdir}/tikv.service
%config(noreplace) %{_sysconfdir}/tikv/*.toml
%config(noreplace) %{_sysconfdir}/sysconfig/tikv
%dir %{_sysconfdir}/tikv
%dir %attr(0755, tikv, tikv) %{_sharedstatedir}/tikv
%dir %attr(0755, tikv, tikv) %{_localstatedir}/log/tikv

%changelog
