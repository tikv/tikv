{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
  hardeningDisable = [ "all" ];
  nativeBuildInputs = with pkgs;[ cmake ];
  buildInputs = with pkgs;[ git gnumake rustup protobuf3_8 perl ];
  shellHooks = ''
    export PROTOC=${pkgs.protobuf3_8}/bin/protoc
  '';
}
