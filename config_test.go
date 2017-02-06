package blehdb

import "testing"

func TestDefautConfig(t *testing.T) {
	c := DefaultConfig()
	if c == nil {
		t.Error("should not have returned nil")
	}
}

func TestValidateConfig(t *testing.T) {
	c := DefaultConfig()

	err := ValidateConfig(c)
	if err == nil {
		t.Error("should have returned an error when StorageDir is empty")
	}

	c.StorageDir = "notempty"
	err = ValidateConfig(c)

	if err != nil {
		t.Error("a valid configuration should have have returned an error")
	}
}
