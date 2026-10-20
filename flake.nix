{
  description = "nattes: a typesafe, in-process message queue inspired by NATS";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { nixpkgs, ... }:
    let
      forAllSystems = nixpkgs.lib.genAttrs [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
    in
    {
      devShells = forAllSystems (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          # Runner for the `instructions` bench. Not in nixpkgs, and its version
          # must match the `gungraun` dev-dependency in Cargo.toml.
          gungraun-runner = pkgs.rustPlatform.buildRustPackage rec {
            pname = "gungraun-runner";
            version = "0.20.0";
            src = pkgs.fetchCrate {
              inherit pname version;
              hash = "sha256-XZHnq0t5XI/lSAdtP0UYxcaaV/YVZOmT/E7YQWX7jOU=";
            };
            cargoHash = "sha256-nYOFsVAuLw13XObUO4mS4edlv5N/BSHRYPPS1N7y0Os=";
            doCheck = false;
          };
        in
        {
          default = pkgs.mkShell {
            packages =
              with pkgs;
              [
                cargo
                rustc
                clippy
                rustfmt
                rust-analyzer
                gnuplot
                mdbook
              ]
              # Valgrind (for the `instructions` bench) is Linux only.
              ++ lib.optionals (lib.meta.availableOn stdenv.hostPlatform valgrind) [
                valgrind
                gungraun-runner
              ];
            RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";
          };
        }
      );

      formatter = forAllSystems (system: nixpkgs.legacyPackages.${system}.nixfmt);
    };
}
