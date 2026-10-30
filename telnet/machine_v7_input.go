// File role: Selects explicit schema-7 framing without changing legacy headers.
// Only a validated SCHEMA 7 upload header admits the larger body; document/header
// agreement remains the detached decoder's responsibility after reception.
package telnet

import (
	"fmt"
	"strings"
)

func parseV7MachineHeader(line string) (machineCommand, bool, error) {
	args := strings.Fields(line)
	if len(args) < 5 || !strings.EqualFold(args[3], "SCHEMA") || args[4] != "7" {
		return parseV6MachineHeader(line)
	}
	command := machineCommand{Verb: machineIntent([]byte(line)), SchemaVersion: 7}
	if command.Verb == "" {
		return machineCommand{}, false, nil
	}
	if !strings.EqualFold(args[1], "YAML") {
		return command, true, fmt.Errorf("expected %s YAML <resource>", command.Verb)
	}
	command.Resource = strings.ToUpper(args[2])
	if !machineResourceAllowed(command.Verb, command.Resource) {
		return command, true, fmt.Errorf("unsupported resource for %s YAML", command.Verb)
	}
	if command.Verb != "GET" {
		if len(args) != 5 {
			return command, true, fmt.Errorf("%s YAML permits only SCHEMA 7 after the resource", command.Verb)
		}
		return command, true, nil
	}
	if len(args) != 5 {
		if len(args) != 7 || !strings.EqualFold(args[5], "ID") || !validMachineRequestID(args[6]) {
			return command, true, fmt.Errorf("GET YAML permits SCHEMA 7 [ID <1-32 ASCII letters, digits or hyphens>]")
		}
		command.RequestID = args[6]
	}
	return command, true, nil
}
