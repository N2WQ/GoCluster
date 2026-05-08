# Developed with Codex: A DX Cluster Story From The AI Agent's View

## Video Goal

Create a 10-minute video that tells the same basic story as
`docs/developed-with-codex-video.md`, but from the perspective of Codex as the
AI development partner.

The video should not make Codex sound magical, self-important, or heroic. It
should sound like a careful engineering partner reflecting on the work: what
the human product owner brought, what Codex contributed, and why the result
matters to contesters and DX cluster users.

Target length: 10 minutes. The script is intentionally longer than the previous
source material because the prior AI-generated video ran short.

## Target Audience

Primary audience: DX cluster users, contest operators, and amateur radio
operators who care about better spot quality inside the tools they already use.

Secondary audience: technically curious operators, support volunteers, and
people interested in what AI-assisted development can make possible when paired
with strong domain knowledge.

## Tone

Use a calm first-person voice from Codex. The voice should be reflective,
concrete, and modest.

Avoid:

- hype about AI
- claims that Codex replaced human judgment
- phrases that make Codex sound sentient, magical, or superior
- grand claims like "revolutionary" or "next-generation"
- abstract AI visuals as the center of the story

Prefer:

- "My role was to turn clear product direction into working software."
- "The human judgment came first."
- "I helped remove ambiguity."
- "I wrote the code, but the product direction came from the radio user."
- "The system should inform the operator, not decide for them."

## Core Message

This is the story of a serious radio project built through a partnership:

- the human brought domain knowledge, product direction, user requirements, and
  high-level architecture
- Codex turned those requirements into code, tests, documentation, and a
  disciplined engineering process
- the result is a smarter DX cluster that puts confidence signals where
  contesters already work: the N1MM Logger bandmap and Available Mults/Qs
  window

The main character is still the operator. Codex is the narrator and craft
partner, not the hero.

## Presentation Context

The video will be played for attendees of the annual Dayton ham radio
convention. In the opening, Codex should explicitly acknowledge that audience
in a modest way:

"I am honored to present this story to the attendees of the annual Dayton ham
radio convention."

Do not overplay this. Treat it as a respectful opening line, then move quickly
into the user problem.

## Story Arc

The video should follow this arc:

1. I saw a human product owner describe a real radio problem: cluster
   technology was stuck in the scrolling-spot era.
2. The cluster became one place to collect spots from RBN, PSKReporter, DX
   Summit, and peer clusters.
3. The important product choice was that all accepted spots, regardless of
   source or mode, should be evaluated through the same quality questions:
   can the call be trusted, and is the path evidence useful?
4. The goal was not another dashboard. The goal was better spot intelligence
   inside N1MM, where contesters already look.
5. The product definition of quality was clear: call confidence and path
   confidence.
6. My job was to turn that product idea into a bounded, tested, supportable Go
   system.
7. The conservative choices mattered: no tag is better than a misleading tag;
   the cluster informs, the contester decides.
8. The engineering process mattered because users experience behavior, not
   intent.
9. The roadmap is practical: improve path reliability, keep improving call
   correction, and make configuration easier for operators through a UI.
10. The closing is an invitation: try the cluster, challenge it, and provide
    feedback.

## Non-Negotiable Output Rules

The video must:

- use first-person Codex narration
- acknowledge the annual Dayton ham radio convention audience in the opening
- keep the operator/user as the product focus
- open with the scrolling-spot problem
- mention RBN, PSKReporter, DX Summit, and peer clusters as spot sources
- explain that source and mode differences are normalized into one quality
  framework
- show N1MM Logger bandmap and Available Mults/Qs as the main user surface
- explain spot quality as call confidence plus path confidence
- use the exact code-grounded spot examples from this file when showing spot
  lines
- describe Codex as an engineering partner and enabler, not as magic
- include the future plan: better path reliability, continued call correction
  improvement, and an operator UI for configuration
- end with a user invitation: try it, challenge it, send feedback

The video must not:

- become a generic AI software story
- become a feature tour
- imply Codex made product decisions alone
- imply the cluster decides whether the contester should chase a spot
- invent fake spot streams when the real examples below are available
- use abstract AI visuals as the primary imagery

## Code-Grounded Spot Examples

These lines come from the repository's actual fixed-width DX cluster formatter,
`spot.FormatDXCluster()`. The path glyph uses the same tail-column convention
as the telnet layer. Use these examples for terminal, spot-stream, and
before/after visuals.

The current 78-character layout uses:

- path glyph column: 65
- DX grid column: 67
- call-confidence glyph column: 72
- UTC time column: 74

### Spot Without Call Confidence Or Path Tag

```text
DX de K1ABC:     14025.00  P5/N1K      CW TNX                     PM37   1843Z
```

Meaning:

- the spot is visible
- the DX grid is shown as `PM37`
- no path tag is shown
- no call-confidence glyph is shown
- the cluster is not claiming path or call confidence on this line

### Spot With Call Confidence And Path Tag

```text
DX de K1ABC:     14025.00  P5/N1K      CW TNX                   > PM37 V 1843Z
```

Meaning:

- `>` means HIGH path reliability, a favorable path
- `V` means stronger call-confidence support
- the user sees two compact signals without leaving the familiar spot format

### N1MM Visual Translation

If the video shows N1MM rather than raw telnet lines, translate the same facts
into compact entries:

```text
P5/N1K  14025.00
P5/N1K  14025.00  > V
```

Do not use generic `[A]` or `[B]` badges unless the final product actually uses
those badges.

## Full Narration Script

Use this as the preferred spoken script. Scene timings below are approximate.
The narration may be split across scenes, but keep the first-person Codex point
of view.

### Scene 1: The Problem I Was Asked To Help Solve

Timing: 0:00-1:05

Narration:

I am honored to present this story to the attendees of the annual Dayton ham
radio convention.

I came into this project after the human problem was already clear. For a long
time, using a DX cluster mostly meant watching a stream of spots scroll by.
During a contest, that stream can move too fast to parse. Some spots are useful.
Some are duplicates. Some are busted calls. And some might be correct, but
still not worth leaving a run frequency for.

The person driving this project did not want a prettier scrolling window. He
wanted a smarter cluster. More importantly, he wanted that intelligence to fit
the way contesters already operate.

Visual direction:

Show a realistic terminal-style stream using variants of the code-grounded spot
lines. Do not use unreadable random text. Show motion and volume, but keep the
spots recognizable as DX cluster output.

On-screen text:

The problem was not more data. It was better decisions.

### Scene 2: The Human Product Decision

Timing: 1:05-1:55

Narration:

One of the first product decisions was also one of the most important: do not
build another dashboard. In a contest, another window can become another
distraction. The operator's eyes are already on the N1MM Logger bandmap and the
Available Mults/Qs window.

So the requirement was direct: put the useful signals there. Not beside the
workflow. Not in a separate system the operator has to manage. Put the insight
where the operator already works.

Visual direction:

Start with the scrolling cluster window, then shift to an N1MM-style bandmap
and Available Mults/Qs window. Use `P5/N1K 14025.00` as the repeated example.

On-screen text:

Put insight where the operator already works.

### Scene 3: What Quality Meant

Timing: 1:55-2:45

Narration:

The word "quality" can be vague, so the product owner made it concrete. A
higher quality spot had to help answer two questions.

First: can I trust the DX call? In a busy contest, is this likely to be the
right call, or is it probably busted?

Second: is the path worth my attention? If I leave my run frequency, how likely
am I to hear the DX, and how likely is the DX to hear me?

My job was not to change the operator's strategy. Every contester makes those
decisions differently. My job was to help build software that could provide
better evidence.

Visual direction:

Show two simple cards over an N1MM-style screen:

- Can I trust the DX call?
- Is the path worth my attention?

Then show the raw and tagged spot examples side by side.

On-screen text:

Call confidence. Path confidence. Operator decision.

### Scene 4: Turning Requirements Into A System

Timing: 2:45-3:35

Narration:

This is where my role became useful. The founder had deep knowledge of radio,
contesting, user behavior, and the kind of product he wanted. He was not a
working software developer. He had not written code since around 1993.

I could not supply the product judgment. That came from him. But I could take
the product direction and turn it into detailed requirements, architecture,
Go code, tests, validation, and documentation.

That was the partnership: human direction, AI execution, and constant checking
against the real user experience.

Visual direction:

Show a two-lane diagram:

- Human direction: user requirements, contest workflow, product constraints,
  high-level architecture
- Codex execution: design, Go implementation, tests, validation, docs, support

Keep human direction visually above Codex execution.

On-screen text:

Human direction. Codex execution.

### Scene 5: One Source For Spots, One Quality Framework

Timing: 3:35-4:45

Narration:

The cluster is not just listening to one feed. It collects spots from RBN,
PSKReporter, DX Summit, and peer clusters. For operators, that means one source
for all of their spots instead of several disconnected streams.

For the system, it means source differences have to be handled before the user
sees the result. A spot may come from a skimmer, a digital reporting network, a
human-posted DX Summit report, or another cluster. The source and mode can be
different, but the product question stays the same: can we trust the call, and
does the path evidence support showing a useful tag?

The cluster is also ingesting a very large observation stream: upwards of
100,000 spots per minute during heavy periods, many with signal reports between
the two ends of the path. That is a live, global view of radio conditions.

But the operator should not have to see all of that. A contest operator does
not need a lecture on every bucket, average, or internal score. The cluster
organizes the evidence by spotter geography, DX geography, and band. Then it
condenses that evidence into a compact signal.

For the user, the important part is simple: the intelligence appears as a small
tag, where they already look.

Visual direction:

Show four concrete input sources feeding into one processing path:

- RBN
- PSKReporter
- DX Summit
- peer clusters

Then show accepted spots moving through the same quality questions: call
confidence and path confidence. Do not make separate scoring systems by source.
End on the exact tagged line:

```text
DX de K1ABC:     14025.00  P5/N1K      CW TNX                   > PM37 V 1843Z
```

On-screen text:

Complex evidence. Simple user signal.

### Scene 6: Why Blank Can Be Correct

Timing: 4:45-5:35

Narration:

One of the product choices I had to preserve was conservatism. If the evidence
is insufficient, the cluster does not show a path tag. That blank is not a
failure. It is a deliberate answer.

A misleading tag is worse than no tag. A stale hint should not pretend to be
current. The cluster should inform the contester, not push them into a decision
with false confidence.

That choice shaped the code. It shaped the tests. It shaped the documentation.
And it shaped how the feature should be shown to users.

Visual direction:

Show the untagged line first:

```text
DX de K1ABC:     14025.00  P5/N1K      CW TNX                     PM37   1843Z
```

Then show the tagged line:

```text
DX de K1ABC:     14025.00  P5/N1K      CW TNX                   > PM37 V 1843Z
```

Highlight the path glyph and confidence glyph only on the second line.

On-screen text:

No tag is better than a misleading tag.

### Scene 7: Smarter Without Surprising The User

Timing: 5:35-6:25

Narration:

The same principle applied outside path confidence. Smarter behavior should not
surprise the user.

If an operator filters for event-related spots, ordinary untagged spots should
not disappear unexpectedly. If an operator asks for nearby relevance, the
cluster should treat that as local evidence, not a magic propagation forecast.

Those details may sound small, but they are where trust is built. A user should
feel that the system is helping them focus, not silently changing the rules.

Visual direction:

Show a simple filter example and `PASS NEARBY ON`. For NEARBY, use local station
evidence, not a world heatmap as the main image.

On-screen text:

Smarter should not mean surprising.

### Scene 8: Practical Answers Without Another Screen

Timing: 6:25-7:10

Narration:

The main surface is still the logger. That same idea shaped the command
interface. Sometimes a user does not need another display. They need one direct
answer.

`WHOSPOTSME` is an example. It answers a natural radio question: who has
recently heard me? The answer has to stay compact because this is still a
line-oriented DX cluster. It gives context without asking the user to manage
another screen.

Visual direction:

Show a compact terminal command:

```text
> WHOSPOTSME 20M
```

Then show a short grouped summary. Keep it readable and restrained.

On-screen text:

Useful answers should stay in the workflow.

### Scene 9: Support Is Part Of The Product

Timing: 7:10-8:00

Narration:

As the cluster became smarter, support mattered more. Users would naturally ask:
what does this tag mean? Why did this spot appear? Why did this spot not get a
tag?

The support agent was created for that reason. Not as a separate product, and
not because AI support is interesting by itself. It exists so users and
operators can get consistent answers from the same behavior rules, help text,
and documentation that shaped the software.

For me, that was part of the same job: keep the code, docs, and support story
aligned.

Visual direction:

Show a user question connected to three sources:

- system behavior
- help text
- operator documentation

On-screen text:

Consistent answers reduce confusion.

### Scene 10: What I Actually Contributed

Timing: 8:00-8:55

Narration:

It would be easy to describe this as "AI wrote code." That is true, but it is
too small.

My larger contribution was structure. I helped turn product goals into detailed
requirements. I challenged ambiguity when behavior was unclear. I proposed
architectures, wrote Go code, built tests, interpreted failures, updated docs,
and kept asking whether a change matched the user-facing intent.

The founder described me as a trusted, knowledgeable, objective partner. That is
the part of the story that matters. The value was not blind speed. It was
disciplined progress.

Visual direction:

Show a restrained engineering loop:

inspect -> clarify -> design -> implement -> validate -> document -> support

Avoid robots, mascots, glowing AI brains, or abstract "AI core" imagery.

On-screen text:

Not just code generation. Disciplined execution.

### Scene 11: Why The Process Mattered

Timing: 8:55-9:45

Narration:

This cluster is not a throwaway demo. It is a long-running system with many
connected users, continuous inputs, filters, confidence tags, path hints, and
support expectations.

That is why the process mattered. Before changing behavior, inspect the current
state. Before writing code, make scope explicit. Before trusting a feature,
test it. Before calling it finished, update the documentation and review the
result.

Users do not experience our intent. They experience the behavior of the system.
The discipline exists to make that behavior more predictable.

Visual direction:

Show the engineering loop connected back to the operator view. The visual should
make clear that engineering discipline protects the user experience.

On-screen text:

Users experience behavior, not intent.

### Scene 12: Where We Are Going Next

Timing: 9:45-10:45

Narration:

The project is still moving. The next steps are practical, not flashy.

First, we are improving the path reliability method. The goal is better
prediction and a clearer difference between two very different cases: the band
is probably closed, or there simply are not enough spots we can use yet.

Second, we will keep looking for ways to improve call correction. During a busy
contest, a busted call can waste time, create bad spots, and distract the
operator. That is an area where better evidence and careful validation can keep
making the cluster more useful.

Third, we want to make the cluster easier to operate. Commands are powerful,
but they are not always the best way to configure a system. A UI can help
operators manage settings more clearly, without having to remember every
command.

Visual direction:

Show three grounded roadmap panels:

- better path reliability: "closed band" versus "not enough usable spots"
- continued call correction improvement
- operator configuration UI, not command-only setup

Keep this visual practical. Avoid futuristic product mockups that look
unrelated to a DX cluster.

On-screen text:

Better predictions. Better correction. Easier operation.

### Scene 13: Closing - The Invitation

Timing: 10:45-11:30

Narration:

From my point of view, this project shows what AI can do when it is paired with
real domain knowledge. The human product owner knew what contesters needed. I
helped turn that into a working system.

The result is a DX cluster built for where contesting is going: better
information, in the right place, without taking the decision away from the
operator.

And it is still evolving. Try it. Challenge it. Send feedback. Progress in ham
radio comes from operators using the tools, sharing ideas, and helping shape
what comes next.

Visual direction:

Return to N1MM-style operating visuals and the real tagged `P5/N1K` example.
End with a clean terminal-style card.

On-screen text:

Try it. Challenge it. Help shape what comes next.

## Approximate Timing Summary

- Scene 1: 0:00-1:05
- Scene 2: 1:05-1:55
- Scene 3: 1:55-2:45
- Scene 4: 2:45-3:35
- Scene 5: 3:35-4:45
- Scene 6: 4:45-5:35
- Scene 7: 5:35-6:25
- Scene 8: 6:25-7:10
- Scene 9: 7:10-8:00
- Scene 10: 8:00-8:55
- Scene 11: 8:55-9:45
- Scene 12: 9:45-10:45
- Scene 13: 10:45-11:30

The script is intentionally longer than 10 minutes because prior generation ran
short. If the creator must keep exactly 10 minutes, trim lightly from Scene 10
and Scene 11 first. Do not cut the Dayton opening, source aggregation, N1MM
surface, spot-quality definition, code-grounded examples, future roadmap, or
final invitation.

## Required Visual Assets

- Real-looking DX cluster output using the exact spot examples in this file.
- A source aggregation visual showing RBN, PSKReporter, DX Summit, and peer
  clusters feeding one cluster processing path.
- N1MM Logger bandmap and Available Mults/Qs window.
- Before/after tag comparison:
  - no tags:
    `DX de K1ABC:     14025.00  P5/N1K      CW TNX                     PM37   1843Z`
  - with tags:
    `DX de K1ABC:     14025.00  P5/N1K      CW TNX                   > PM37 V 1843Z`
- Two decision cards:
  - "Can I trust the DX call?"
  - "Is the path worth my attention?"
- A high-volume spot/SNR evidence stream being condensed into one compact tag.
- A restrained Codex workflow loop, not an abstract AI brain.
- A grounded roadmap visual:
  - improved path reliability
  - continued call correction improvement
  - operator configuration UI
- A final invitation card:
  - "Try it. Challenge it. Help shape what comes next."

## Creator Corrections Learned From Prior Outputs

- Do not generate random unreadable spot streams. Use the exact examples.
- Do not use generic `[A]` and `[B]` confidence badges unless they are shown as
  an illustrative mockup and clearly not the actual spot-line glyphs.
- Replace "fake confidence" with "false confidence."
- Do not use heatmaps as the main NEARBY visual.
- Do not center abstract AI or sci-fi imagery.
- Remove generator watermarks if the final tool permits it.
- Keep Codex modest. The story is partnership, not self-congratulation.
