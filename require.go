package kstreams

func requireString(item string, msg string) {
	if item == "" {
		panic(msg)
	}
}

func requireStringArray(items []string, msg string) {
	if len(items) == 0 {
		panic(msg)
	}
	for _, item := range items {
		if item == "" {
			panic(msg)
		}
	}
}
