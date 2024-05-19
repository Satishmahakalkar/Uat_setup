import argparse
import subprocess
import settings
from tortoise import Tortoise, run_async


if __name__ == "__main__":
    parser = argparse.ArgumentParser("ManageDeployment")
    parser.add_argument("-d", "--bundle-with-deps", action='store_true')
    parser.add_argument("-b", "--bundle", action='store_true')
    parser.add_argument("--tortoise-init", action='store_true')
    parser.add_argument("--aerich", nargs='+')
    args = parser.parse_args()
    print(args)
    if args.bundle_with_deps or args.bundle:
        subprocess.run("git archive HEAD -o deployment/bundle/app.zip", shell=True)
        subprocess.run("zip -g deployment/bundle/app.zip settings/production.py", shell=True)
        if args.bundle_with_deps:
            subprocess.run("zip -g deployment/bundle/app.zip -r .venv/lib/python3.8/site-packages/* -x '*/__pycache__/*' -x '*aws_cdk/*' -x '*jsii/*' -x '*/tests/*'", shell=True)
            subprocess.run("zipnote deployment/bundle/app.zip | grep 'site-packages' > /tmp/zipnames.txt", shell=True)
            with open("/tmp/zipnames.txt", "r") as fp:
                lines = fp.readlines()
            lines2 = []
            for line in lines:
                lines2.append(line)
                line = line.replace(' ', '=')
                line = line.replace('.venv/lib/python3.8/site-packages/', '')
                lines2.append(line)
                lines2.append("@ (comment above this line)\n")
            with open("/tmp/zipnames.txt", "w") as fp:
                fp.writelines(lines2)
            subprocess.run("zipnote -w deployment/bundle/app.zip < /tmp/zipnames.txt", shell=True)
    elif args.tortoise_init:
        run_async(Tortoise.init(settings.TORTOISE_ORM))
    elif args.aerich:
        run_async(Tortoise.init(settings.TORTOISE_ORM))
        subprocess.run(["aerich"] + args.aerich)