// File role: Pure generation-6 headers for the active machine dispatcher.
package telnet

import (
	"fmt"
	"strings"
)

func parseV6MachineHeader(line string) (machineCommand, bool, error) {
	command := machineCommand{Verb: machineIntent([]byte(line))}
	if command.Verb == "" {
		return command, false, nil
	}
	args := strings.Fields(line)
	if len(args) < 3 || !strings.EqualFold(args[1], "YAML") {
		return command, true, fmt.Errorf("expected %s YAML <resource>", command.Verb)
	}
	command.Resource = strings.ToUpper(args[2])
	if !machineResourceAllowed(command.Verb, command.Resource) {
		return command, true, fmt.Errorf("unsupported resource for %s YAML", command.Verb)
	}
	if command.Verb != "GET" {
		if len(args) != 3 {
			return command, true, fmt.Errorf("%s YAML accepts no additional header arguments", command.Verb)
		}
		return command, true, nil
	}
	if len(args) < 5 || !strings.EqualFold(args[3], "SCHEMA") || args[4] != "6" {
		return command, true, fmt.Errorf("GET YAML requires explicit SCHEMA 6")
	}
	command.SchemaVersion = 6
	if len(args) != 5 {
		if len(args) != 7 || !strings.EqualFold(args[5], "ID") || !validMachineRequestID(args[6]) {
			return command, true, fmt.Errorf("GET YAML permits SCHEMA 6 [ID <1-32 ASCII letters, digits or hyphens>]")
		}
		command.RequestID = args[6]
	}
	return command, true, nil
}
