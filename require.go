package kstreams

func requireString(v, msg string) error {
	if v == "" {
		panic(msg)
	}

	return nil
}
