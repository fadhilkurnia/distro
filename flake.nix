{
    description = "Distrobench Python development environment";

    inputs = {
	nixpkgs.url = "github:nixos/nixpkgs/nixos-25.11";
    };

    outputs = { self, nixpkgs }:
	let
	system = "x86_64-linux";
	pkgs = nixpkgs.legacyPackages.${system};
    in
    {
	devShells.${system}.default = pkgs.mkShell {
	    packages = with pkgs; [
		(python3.withPackages (ps: [
				       ps.python-dotenv
				       ps.packaging
		]))
		    rustc
		    cargo
		    javaPackages.compiler.openjdk21
		    maven
	    ];

	    shellHook = ''
		# Use a local .m2 folder instead of the global home one
		export MAVEN_OPTS="-Dmaven.repo.local=$(pwd)/.m2/repository"

		echo "Distrobench Python Dev Environment Loaded"
		python --version
		'';
	};
    };
}
