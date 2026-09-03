# Union of what every script in this project directly invokes:
#   build.sh              - ant (jar build), go (CLI cross-compile)
#   start-control.sh /
#   stop-control.sh        - gpServer.sh itself needs a JVM to run the
#                            gigapaxos process, and rsync/ssh for its own
#                            internal distribution to other nodes
#   ensure-provisioned.sh  - ssh, to reach every other node itself
#   run-latency.sh         - k6
#   launch-service.sh      - none beyond the xdn CLI binary itself (already
#                            statically built), included here only because
#                            every script in this project shares one file
#   set-placement.sh /
#   probe-replica.sh       - curl, to talk to the control plane and to a
#                            specific replica's own address
{ pkgs ? import ../../nix/pkgs.nix }:
pkgs.mkShell {
    packages = [ pkgs.jdk21 pkgs.ant pkgs.go pkgs.k6 pkgs.rsync pkgs.openssh pkgs.curl ];
}
