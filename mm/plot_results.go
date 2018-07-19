package main

// --------------------------------------------------------------------------------
// Plot results
// --------------------------------------------------------------------------------

//import csv
//from matplotlib.pyplot import *
//
//merged_report_filepath = 'data/test_run_001_merged_report.csv'
//replicate_ids = ['r1','r2','r3']
//rowdicts = []
//
//
//# Collect data in one dict per row in the csv file
//with open(merged_report_filepath) as infile:
//    csvrd = csv.reader(infile, delimiter=',')
//    for rid, row in enumerate(csvrd):
//        if rid == 0:
//            headerrow = row
//        else:
//            rowdicts.append({headerrow[i]:v for i, v in enumerate(row)})
//
//# Collect the training sizes
//train_sizes = []
//for r  in rowdicts:
//    if r['replicate_id'] == 'r1':
//        train_sizes.append(r['train_size'])
//
//# Collect the training times, RMSD- and (LIBLINEAR) Cost values
//train_times = {}
//rmsd_values = {}
//cost_values = {}
//for repl_id in replicate_ids:
//    train_times[repl_id] = []
//    rmsd_values[repl_id] = []
//    cost_values[repl_id] = []
//    for r in rowdicts:
//        if r['replicate_id'] == repl_id:
//            train_times[repl_id].append(r['train_time_sec'])
//            rmsd_values[repl_id].append(r['rmsd'])
//            cost_values[repl_id].append(r['lin_cost'])
//
//# Calculate average values for the training time
//train_times_avg = []
//for i in range(0, len(train_times['r1'])):
//    train_times_avg.append(0.0)
//    for repl_id in replicate_ids:
//        train_times_avg[i] += float(train_times[repl_id][i])
//    train_times_avg[i] =  train_times_avg[i] / float(len(replicate_ids))
//
//# Calculate average values for the RMSD values
//rmsd_values_avg = []
//for i in range(0, len(rmsd_values['r1'])):
//    rmsd_values_avg.append(0.0)
//    for repl_id in replicate_ids:
//        rmsd_values_avg[i] += float(rmsd_values[repl_id][i])
//    rmsd_values_avg[i] =  rmsd_values_avg[i] / float(len(replicate_ids))
//
//
//# ## Print values (Train sizes, train times and RMSD)
//
//# In[ ]:
//
//print "-"*60
//print 'Train sizes:        ' + ', '.join(train_sizes) + ' molecules'
//print ''
//print 'RMSD values: '
//for rid in range(1,4):
//    print '      Replicate %d: ' % rid + ', '.join(['%.2f' % float(v) for v in rmsd_values['r%d' % rid]])
//print ''
//print 'Train times: '
//for rid in range(1,4):
//    print '      Replicate %d: ' % rid + ', '.join(train_times['r%d' % rid]) + ' seconds'
//print ''
//print 'Cost values: '
//for rid in range(1,4):
//    print '      Replicate %d: ' % rid + ', '.join(cost_values['r%d' % rid])
//print ''
//print 'RMSD values (avg): ' + ', '.join(['%.2f' % x for x in rmsd_values_avg])
//print 'Train times (avg): ' + ', '.join(['%.2f' % x for x in train_times_avg]) + ' seconds'
//print "-"*60
//
//
//# ## Plot train time and RMSD against training size
//
//# In[ ]:
//
//# Initialize plotting figure
//fig = figure()
//
//# Set up subplot for RMSD values
//subpl1 = fig.add_subplot(1,1,1)
//# x-axis
//xticks = [500,1000,2000,4000,8000]
//subpl1.set_xscale('log')
//subpl1.set_xlim([500,8000])
//subpl1.set_xticks(ticks=xticks)
//subpl1.set_xticklabels([str(l) for l in xticks])
//subpl1.set_xlabel('Training set size (number of molecules)')
//# y-axis
//subpl1.set_ylim([0,1])
//subpl1.set_ylabel('RMSD for test prediction')
//# plot
//subpl1.plot(train_sizes,
//     rmsd_values_avg,
//     label='RMSD for test prediction',
//     marker='.',
//     color='k',
//     linestyle='-')
//
//# Set up subplot for training times
//subpl2 = subpl1.twinx()
//# y-axis
//yticks = [0.01,0.02,0.03,0.05,0.1,0.2,0.3,0.4,0.5]
//subpl2.set_ylim([0.01,0.5])
//subpl2.set_yscale('log')
//subpl2.set_yticks(ticks=yticks)
//subpl2.set_yticklabels([str(int(l*1000)) for l in yticks])
//subpl2.set_ylabel('Training time (milliseconds)')
//subpl2.tick_params(axis='y', colors='r')
//subpl2.yaxis.label.set_color('r')
//subpl2.spines['right'].set_color('red')
//# plot
//subpl2.plot(train_sizes,
//     train_times_avg,
//     label='Training time (seconds)',
//     marker='.',
//     color='r',
//     linestyle='-')
//
//subpl1.legend(loc='upper left', fontsize=9)
//subpl2.legend(bbox_to_anchor=(0, 0.9), loc='upper left', fontsize=9)
//
//show() # Display the plot
