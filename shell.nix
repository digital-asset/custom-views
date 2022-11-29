# Update nixpkgs with:
# nix-shell -p niv --run "niv update"

let
  sources = import ./nix/sources.nix;
  pkgs = import sources.nixpkgs {};
in
pkgs.mkShell {
  buildInputs = [
    pkgs.bash
    pkgs.git
    pkgs.gnupg
    (pkgs.sbt.override { jre = pkgs.openjdk11_headless; })
  ];
}
