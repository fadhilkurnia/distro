# Throwaway devShell used only to validate the nix.Run mechanism.
# pkgs.hello is the classic Nix tutorial package. Its only job is
# printing a greeting, which makes it a clean way to prove nix-shell
# actually provisioned something, rather than proving ambient bash
# already worked (which testdata/echo.sh alone wouldn't distinguish).
{ pkgs ? import ../../nix/pkgs.nix }:
pkgs.mkShell {
  packages = [ pkgs.hello ];
}
