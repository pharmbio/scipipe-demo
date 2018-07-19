package main

//class CollectDataReportRow(sl.Task):
//    dataset_name = luigi.Parameter()
//    train_method = luigi.Parameter()
//    train_size = luigi.Parameter()
//    replicate_id = luigi.Parameter()
//    lin_cost = luigi.Parameter()
//
//    in_rmsd = None
//    in_traintime = None
//    in_trainsize_filtered = None
//
//    def out_datareport_row(self):
//        outdir = os.path.dirname(self.in_rmsd().path)
//        return sl.TargetInfo(self, os.path.join(outdir, '{ds}_{lm}_{ts}_{ri}_datarow.txt'.format(
//                    ds=self.dataset_name,
//                    lm=self.train_method,
//                    ts=self.train_size,
//                    ri=self.replicate_id
//                )))
//
//    def run(self):
//        with self.in_rmsd().open() as rmsdfile:
//            rmsddict = sl.recordfile_to_dict(rmsdfile)
//            rmsd = rmsddict['rmsd']
//
//        with self.in_traintime().open() as traintimefile:
//            train_time_sec = traintimefile.read().rstrip('\n')
//
//        with self.in_trainsize_filtered().open() as trainsizefile:
//            train_size_filtered = trainsizefile.read().strip('\n')
//
//        if self.lin_cost is not None:
//            lin_cost = self.lin_cost
//        else:
//            lin_cost = 'NA'
//
//        with self.out_datareport_row().open('w') as outfile:
//            rdata = { 'dataset_name': self.dataset_name,
//                      'train_method': self.train_method,
//                      'train_size': self.train_size,
//                      'train_size_filtered': train_size_filtered,
//                      'replicate_id': self.replicate_id,
//                      'rmsd': rmsd,
//                      'train_time_sec': train_time_sec,
//                      'lin_cost': lin_cost}
//            sl.dict_to_recordfile(outfile, rdata)

//class CollectDataReport(sl.Task):
//    dataset_name = luigi.Parameter()
//    train_method = luigi.Parameter()
//
//    in_datareport_rows = None
//
//    def out_datareport(self):
//        outdir = os.path.dirname(self.in_datareport_rows[0]().path)
//        return sl.TargetInfo(self, os.path.join(outdir, '{ds}_{tm}_datareport.csv'.format(
//                    ds=self.dataset_name,
//                    tm=self.train_method
//               )))
//
//    def run(self):
//        with self.out_datareport().open('w') as outfile:
//            csvwrt = csv.writer(outfile)
//            # Write header
//            csvwrt.writerow(['dataset_name',
//                             'train_method',
//                             'train_size',
//                             'train_size_filtered',
//                             'replicate_id',
//                             'rmsd',
//                             'train_time_sec',
//                             'lin_cost'])
//            # Write data rows
//            for intargetinfofunc in self.in_datareport_rows:
//                with intargetinfofunc().open() as infile:
//                    r = sl.recordfile_to_dict(infile)
//                    csvwrt.writerow([r['dataset_name'],
//                                     r['train_method'],
//                                     r['train_size'],
//                                     r['train_size_filtered'],
//                                     r['replicate_id'],
//                                     r['rmsd'],
//                                     r['train_time_sec'],
//                                     r['lin_cost']])

//class CalcAverageRMSDForCost(sl.Task): # TODO: Check with Jonalv whether RMSD is what we want to do?!!
//    # Parameters
//    lin_cost = luigi.Parameter()
//
//    # Inputs
//    in_assessments = None
//
//    # output
//    def out_rmsdavg(self):
//        return sl.TargetInfo(self, self.in_assessments[0]().path + '.avg')
//
//    def run(self):
//        vals = []
//        for invalfun in self.in_assessments:
//            infile = invalfun().open()
//            records = sl.util.recordfile_to_dict(infile)
//            vals.append(float(records['rmsd']))
//        rmsdavg = sum(vals)/len(vals)
//        rmsdavg_records = {'rmsd_avg': rmsdavg,
//                           'cost': self.lin_cost}
//        with self.out_rmsdavg().open('w') as outfile:
//            sl.util.dict_to_recordfile(outfile, rmsdavg_records)

//class SelectLowestRMSD(sl.Task):
//    # Inputs
//    in_values = None
//
//    # output
//    def out_lowest(self):
//        cost_part = '.c' + hashlib.md5('_'.join([v().task.lin_cost for v in self.in_values])).hexdigest()
//        return sl.TargetInfo(self, self.in_values[0]().path + cost_part + '.min')
//
//    def run(self):
//        vals = []
//        for invalfun in self.in_values:
//            infile = invalfun().open()
//            records = sl.util.recordfile_to_dict(infile)
//            vals.append(records)
//
//        lowest_rmsd = float(min(vals, key=lambda v: float(v['rmsd_avg']))['rmsd_avg'])
//        vals_lowest_rmsd = [v for v in vals if float(v['rmsd_avg']) <= lowest_rmsd]
//        val_lowest_rmsd_cost = min(vals_lowest_rmsd, key=lambda v: v['cost'])
//        lowestrec = {'lowest_rmsd_avg': val_lowest_rmsd_cost['rmsd_avg'],
//                     'lowest_cost': val_lowest_rmsd_cost['cost']}
//        with self.out_lowest().open('w') as lowestfile:
//            sl.util.dict_to_recordfile(lowestfile, lowestrec)

//class CreateFolds(sl.SlurmTask):
//
//    # TASK PARAMETERS
//    folds_count = luigi.IntParameter()
//    fold_index = luigi.IntParameter()
//
//    # TARGETS
//    in_dataset = None
//    in_linecount = None
//
//    def out_testdata(self):
//        return sl.TargetInfo(self, self.in_dataset().path + '.fld{0:02}_tst'.format(self.fold_index))
//
//    def out_traindata(self):
//        return sl.TargetInfo(self, self.in_dataset().path + '.fld{0:02}_trn'.format(self.fold_index))
//
//    def run(self):
//        with self.in_linecount().open() as linecntfile:
//            linecnt = int(linecntfile.read())
//
//        linesperfold = int(math.floor(linecnt / self.folds_count))
//        tst_start = self.fold_index * linesperfold
//        tst_end = (self.fold_index + 1) * linesperfold
//
//        # CREATE TEST FOLD
//        self.ex(['awk',
//                 '"NR >= %d && NR <= %d { print }"' % (tst_start, tst_end),
//                 self.in_dataset().path,
//                 '>',
//                 self.out_testdata().path])
//
//        # CREATE TRAIN FOLD
//        self.ex(['awk',
//                 '"NR < %d || NR > %d { print }"' % (tst_start, tst_end),
//                 self.in_dataset().path,
//                 '>',
//                 self.out_traindata().path])

//class SelectPercentIndexValue(sl.Task):
//
//    # TASK PARAMETERS
//    percent_index = luigi.IntParameter()
//
//    # TARGETS
//    in_prediction = None
//
//    def out_indexvalue(self):
//        return sl.TargetInfo(self, self.in_prediction().path + '.idx{i:d}'.format(i=self.percent_index))
//
//    def run(self):
//        with self.in_prediction().open() as infile:
//            lines = [float(l) for l in infile.readlines()]
//            lines.sort()
//            linescnt = len(lines)
//            index = int(linescnt * (self.percent_index / 100.0))
//            indexval = lines[index]
//            with self.out_indexvalue().open('w') as outfile:
//                outfile.write('%f\n' % indexval)

//class MergedDataReport(sl.Task):
//    run_id = luigi.Parameter()
//
//    in_reports = None
//
//    def out_merged_report(self):
//        return sl.TargetInfo(self, 'data/' + self.run_id + '_merged_report.csv')
//
//    def run(self):
//        merged_rows = []
//        for i, inreportfile_targetinfo in enumerate(self.in_reports):
//            infile = inreportfile_targetinfo().open()
//            for j, line in enumerate(infile):
//                if i == 0 and j == 0:
//                    merged_rows.append(line) # Append header
//                if j > 0:
//                    merged_rows.append(line)
//        with self.out_merged_report().open('w') as outfile:
//            outfile.write(''.join(merged_rows))
//class MergeOrigAndPredValues(sl.Task):
//    # TARGETS
//    in_original_dataset = lambda: sl.TargetInfo(None, None)
//    in_predicted_dataset = lambda: sl.TargetInfo(None, None)
//
//    def out_merged(self):
//        return sl.TargetInfo(self, self.in_original_dataset().path + '.merged')
//
//    def run(self):
//        with self.in_original_dataset().open() as origfile:
//            with self.in_predicted_dataset().open() as predfile:
//                with self.out_merged().open('w') as outfile:
//                    for orig, pred in zip(origfile, predfile):
//                        outfile.write(orig.split(' ')[0] + ', ' + pred + '\n')
//class MergedDataReport(sl.Task):
//    run_id = luigi.Parameter()
//
//    in_reports = None
//
//    def out_merged_report(self):
//        return sl.TargetInfo(self, 'data/' + self.run_id + '_merged_report.csv')
//
//    def run(self):
//        merged_rows = []
//        for i, inreportfile_targetinfo in enumerate(self.in_reports):
//            infile = inreportfile_targetinfo().open()
//            for j, line in enumerate(infile):
//                if i == 0 and j == 0:
//                    merged_rows.append(line) # Append header
//                if j > 0:
//                    merged_rows.append(line)
//        with self.out_merged_report().open('w') as outfile:
//            outfile.write(''.join(merged_rows))

//class PlotCSV(sl.Task):
//    # TARGETS
//    in_csv = lambda: sl.TargetInfo(None, None)
//
//    xmin = luigi.Parameter()
//    xmax = luigi.Parameter()
//    ymin = luigi.Parameter()
//    ymax = luigi.Parameter()
//
//    def out_pdf(self):
//        return sl.TargetInfo(self, self.in_csv().path + '.pdf')
//
//    def run(self):
//        # Create a temporary R script
//        rscript = u'''
//        ## Parse arguments
//        library('argparse')
//        p <- ArgumentParser()
//        p$add_argument("-i", "--input", type="character",
//                       help="Input file in CSV format")
//        p$add_argument("-o", "--output", type="character",
//                       help="Output file (will be in .pdf format)")
//        args <- p$parse_args()
//
//        ## Plot
//        if ( args$input != "" && args$output != "" ) {{
//          data = read.csv(file=args$input, header = FALSE)
//          pdf(file = args$output, width=5, height=5)
//          plot(NULL, xlim=c({xmin},{xmax}), ylim=c({ymin},{ymax}), xlab="", ylab="", cex.axis=1.5)
//          points(data, cex = .2, pch=16)
//          dev.off()
//        }} else {{
//            print('Either input or output is missing! Use -h to see options!')
//            quit(1)
//        }}
//        '''.format(
//                xmin=self.xmin,
//                xmax=self.xmax,
//                ymin=self.ymin,
//                ymax=self.ymax)
//
//        tempscriptpath='.temp-r-script-%s.r' % uuid.uuid4()
//        tsf = open(tempscriptpath,'w')
//        tsf.write(rscript)
//        tsf.close()
//        # Execute the R script
//        self.ex_local(['xvfb-run',
//                       'Rscript',
//                       tempscriptpath,
//                       '-i',
//                       self.in_csv().path,
//                       '-o',
//                       self.out_pdf().path])
//        # Remove the temporary R script
//        self.ex_local(['rm',
//                       tempscriptpath])

//class MergedDataReport(sl.Task):
//    run_id = luigi.Parameter()
//
//    in_reports = None
//
//    def out_merged_report(self):
//        return sl.TargetInfo(self, 'data/' + self.run_id + '_merged_report.csv')
//
//    def run(self):
//        merged_rows = []
//        for i, inreportfile_targetinfo in enumerate(self.in_reports):
//            infile = inreportfile_targetinfo().open()
//            for j, line in enumerate(infile):
//                if i == 0 and j == 0:
//                    merged_rows.append(line) # Append header
//                if j > 0:
//                    merged_rows.append(line)
//        with self.out_merged_report().open('w') as outfile:
//            outfile.write(''.join(merged_rows))

// ================================================================================
// TEMPLATE
// ================================================================================

// // REPLACETHIS does blabla ...
// type REPLACETHIS struct {
// 	*sp.Process
// }
//
// // REPLACETHISConf contains parameters for initializing a
// // REPLACETHIS process
// type REPLACETHISConf struct {
// }
//
// // NewREPLACETHIS returns a new REPLACETHIS process
// func NewREPLACETHIS(wf *sp.Workflow, name string, params REPLACETHISConf) *REPLACETHIS {
// 	cmd := ``
// 	p := wf.NewProc(name, cmd)
// 	p.SetOut("out", "out.txt")
// 	return &REPLACETHIS{p}
// }
//
// // InInfile returns the Infile in-port
// func (p *REPLACETHIS) InInfile() *sp.InPort {
// 	return p.In("in")
// }
//
// // OutOutfile returns the Outfile out-port
// func (p *REPLACETHIS) OutOutfile() *sp.OutPort {
// 	return p.Out("out")
// }
