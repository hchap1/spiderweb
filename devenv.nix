{ pkgs, ... }:

{
  languages.rust.enable = true;

  packages = [
    pkgs.pkg-config
  ];
}
