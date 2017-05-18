package main

import (
	sp "github.com/scipipe/scipipe"
)

func main() {
	sp.InitLogInfo()

	runner := sp.NewPipelineRunner()

	// Init processes

	dlApps := sp.NewFromShell("dlApps", "wget http://uppnex.se/apps.tar.gz -O {o:apps}")
	dlApps.SetPathStatic("apps", "uppnex_apps.tar.gz")
	runner.AddProcess(dlApps)

	unzipApps := sp.NewFromShell("unzipApps", "zcat {i:targz} > {o:tar}")
	unzipApps.SetPathReplace("targz", "tar", ".gz", "")
	runner.AddProcess(unzipApps)

	untarApps := sp.NewFromShell("untarApps", "tar -xvf {i:tar} # {o:outdir}")
	untarApps.SetPathStatic("outdir", "apps")
	runner.AddProcess(untarApps)

	sink := sp.NewSink()
	runner.AddProcess(sink)

	// Connect network

	unzipApps.In["targz"].Connect(dlApps.Out["apps"])
	untarApps.In["tar"].Connect(unzipApps.Out["tar"])
	sink.Connect(untarApps.Out["outdir"])

	runner.Run()
}
