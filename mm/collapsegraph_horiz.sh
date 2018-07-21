cat \
    <(head -n 5 mmdag.dot) \
    <(tail -n +6 mmdag.dot | head -n -1 | sed -r 's/_tr[0-9]+/_tr{500-1000}/g' | sed -r 's/_c[0-9\.]+/_c{0.0001-5.000}/g'| sed -r 's/_fld[0-9]+/_fld{1-10}/g' | sort | uniq) \
    <(tail -n 1 mmdag.dot) \
    | sed -r 's/_/\\n/g' \
    > mmdag.collapsed.horiz.dot
