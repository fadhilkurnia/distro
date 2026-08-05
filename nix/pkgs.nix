# Shared, pinned nixpkgs snapshot. Most protocols' shell.nix reference
# this by default (import ../../nix/pkgs.nix) rather than each pinning
# their own.
# Any protocol with a genuine need for a different nixpkgs revision (e.g.
# a package version that's aged out of this snapshot) is free to pin its
# own instead.
#
# Pinned to nixpkgs nixos-25.11 branch, at the commit current as of this
# writing:
#   commit: b6018f87da91d19d0ab4cf979885689b469cdd41
#   fetched + sha256 verified directly against github.com/NixOS/nixpkgs
#
# To bump this pin later: pick a newer commit from the nixos-25.11 branch
# (or a different branch/tag entirely), then compute its hash with:
#   nix-prefetch-url --unpack https://github.com/NixOS/nixpkgs/archive/<commit>.tar.gz
import (fetchTarball {
  url = "https://github.com/NixOS/nixpkgs/archive/b6018f87da91d19d0ab4cf979885689b469cdd41.tar.gz";
  sha256 = "0ln4yw7z3g9lb0x081hc0pd2j1wsx2qqf6bgmwwvdbkcl4bcy1dp";
}) {}
