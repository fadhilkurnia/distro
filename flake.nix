{
    description = "Distrobench Python development environment";

    inputs = {
	nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    };

    outputs = { self, nixpkgs }:
	let
	system = "x86_64-linux";
	pkgs = nixpkgs.legacyPackages.${system};
    in
    {
	devShells.${system}.default = pkgs.mkShell {
	    packages = [
		(pkgs.python3.withPackages (ps: [
					    ps.python-dotenv
					    ps.packaging
		]))
	    ];

	    shellHook = ''
		echo "Distrobench Python Dev Environment Loaded"
		python --version
		'';
	};
    };
}
