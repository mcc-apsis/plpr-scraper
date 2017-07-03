pkg_name=api
pkg_version=0.1.0
pkg_maintainer="Datenschule"
pkg_license=("All Rights Reserved")
pkg_deps=(
  core/git
  core/postgresql
  core/python/3.5.2
)
pkg_origin=k-nut
pkg_build_deps=()
pkg_bin_dirs=(bin)
pkg_lib_dirs=(lib)
pkg_include_dirs=(include)
pkg_exports=()
pkg_exposes=()
pkg_svc_user="root"

do_download(){
  return 0
}

do_verify(){
  return 0
}

do_unpack(){
  return 0
}

do_prepare(){
  pip install --upgrade pip
  return $?
}

do_build(){
  return 0
}

do_install(){
  cd /src
  pip install -r requirements.txt
  pip install -e .
  return $?
}

do_strip() {
  return 0
}

