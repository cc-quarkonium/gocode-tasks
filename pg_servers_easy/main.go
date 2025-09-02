package main

func main() {
	// tests := append(testCases, privateTestCases...)
	tests := testCases

	for _, tt := range tests {
		CustomTestBody(
			tt.name,
			func() struct{} {
				return tt.prepare()
			},
			func(_ struct{}) bool {
				return tt.check(tt.full)
			},
		)
	}
}
