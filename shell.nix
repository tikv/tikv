{ pkgs ? import <nixpkgs> {} }:
pkgs.mkShell {
  hardeningDisable = [ "all" ];
  nativeBuildInputs = with pkgs;[ cmake ];
  buildInputs = with pkgs;[ gnumake rustup protobuf3_8 ];
  shellHooks = ''
    export PROTOC=${pkgs.protobuf3_8}/bin/protoc
  '';
}
