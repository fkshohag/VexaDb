package index

import "strconv"

func strconvItoa(i int) string         { return strconv.Itoa(i) }
func strconvFormat(f float64) string   { return strconv.FormatFloat(f, 'f', -1, 64) }
