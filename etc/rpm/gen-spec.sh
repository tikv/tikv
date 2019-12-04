cat <<-EOF
%global __os_install_post /usr/lib/rpm/brp-compress %{nil}
%global __spec_install_post %{nil}
%global debug_package %{nil}

Name:           tikv
Version:        4.0.0.alpha
Release:        1%{?dist}
Summary:        Distributed transactional key value database powered by Rust and Raft

License:        Apache License 2.0
URL:            https://github.com/tikv/tikv

Source0:        tikv-server
Source1:        tikv-ctl
Source2:        tikv.service
Source3:        config.toml

Requires:       systemd
Requires(pre):  shadow-utils
Requires(post): systemd

%description
A distributed transactional key-value database.

%install
%{__install} -D -m 0755 -o root -g root %{SOURCE0} \$RPM_BUILD_ROOT%{_bindir}/tikv-server
%{__install} -D -m 0755 -o root -g root %{SOURCE1} \$RPM_BUILD_ROOT%{_bindir}/tikv-ctl
%{__install} -D -m 0644 -o root -g root %{SOURCE2} \$RPM_BUILD_ROOT%{_unitdir}/tikv.service
%{__install} -D -m 0644 -o root -g root %{SOURCE3} \$RPM_BUILD_ROOT/etc/tikv/config.toml
%{__mkdir} -p \$RPM_BUILD_ROOT/%{_sharedstatedir}/tikv

%pre
if ! getent passwd tikv >/dev/null; then
        adduser --system --no-create-home --home /etc/tikv --shell /usr/sbin/nologin tikv
fi
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
%dir %{_sysconfdir}/tikv
%dir %attr(0755, tikv, tikv) %{_sharedstatedir}/tikv

%changelog
EOF
