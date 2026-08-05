# Paxi only needs Go — repo cloning/checkout is handled entirely in Go
# via go-git now, so unlike the original Python approach, git itself
# doesn't need to be a declared dependency here.
{ pkgs ? import ../../nix/pkgs.nix }:
pkgs.mkShell {
  packages = [ pkgs.go ];
}
