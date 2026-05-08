# Developed with Codex: Building a Smarter DX Cluster

## Video Goal

Create a 10-minute user-first video that explains how this DX cluster was
developed with Codex. The video should make the cluster feel useful,
trustworthy, and familiar to users, while showing that AI-assisted development
was used to improve the product, the engineering process, and the support
experience.

## Target Audience

Primary audience: DX cluster users and amateur radio operators who want cleaner
spots, better filters, and practical commands.

Secondary audience: system operators, support volunteers, and technically
curious users who care about reliability, diagnostics, and how the system is
maintained.

## Tone

Natural, practical, and conversational. This should sound like a real person
explaining what was built and why it matters, not like a polished AI marketing
script. Avoid hype, grand claims, buzzwords, and overly dramatic phrasing. The
story is not that AI replaced engineering judgment. The story is that Codex
helped turn user needs into clearer decisions, better code, and more consistent
support.

Use plain spoken language. Short sentences are better than polished slogans.
It is fine to say "the system does not know yet" or "that would be misleading"
instead of using abstract phrases such as "optimized confidence signaling."

## Core Message

This cluster was created because traditional DX cluster use still felt stuck in
the scrolling-spot era. The goal was to build a smarter cluster from the ground
up: one that improves spot quality, feeds confidence tags into the N1MM Logger
bandmap and Available Mults/Qs window, and lets contesters make faster decisions
inside the tools they already use.

Codex made the project possible. The human role was product direction, user
requirements, and high-level architecture. Codex handled the development:
turning requirements into detailed designs, removing ambiguity, writing the Go
code, creating tests, interpreting validation results, and keeping the
documentation aligned.

## Story Arc

This video should tell a user-centered story, not a feature list.

The main character is the contester trying to make good radio decisions without
watching a scrolling wall of spots.

The conflict is noise, uncertainty, and surprise:

- too many spots moving too quickly
- cluster technology that still assumes users watch a scrolling stream
- uncertainty about whether the DX call is correct
- uncertainty about whether leaving the run frequency is worth it
- filters that can hide more than the user expected
- path hints that could sound more certain than the evidence allows
- local activity that is hard to separate from global activity
- support questions that take too long to answer

The resolution is a cluster that stays familiar but behaves more carefully:

- confidence tags appear where contesters already look: the N1MM bandmap and
  Available Mults/Qs window
- spot quality is defined from the user's point of view: call confidence and
  path confidence
- real-time SNR observations are condensed into simple hints
- ordinary spots do not vanish unexpectedly
- stale or weak path evidence is treated honestly
- nearby activity is framed as local evidence, not prediction
- practical commands answer real radio questions
- operators get quiet tools that protect the user experience
- the support agent gives faster, more consistent answers

Codex belongs in the craft layer of the story. It wrote the code and made the
development possible, but human judgment set the direction: the product goals,
the user experience, and the high-level architecture.

The emotional landing is trust. The best DX cluster is not the one with the
most technology visible. It is the one that gives users useful information,
explains itself when needed, and stays reliable under real operating
conditions. The closing should also invite users into the future: try the
cluster, challenge it, and provide feedback, because progress in ham radio
depends on user ideas and real operating experience.

## Interview Anchors

Use these interview-derived points as the human spine of the video. They should
shape narration, slide order, and transitions:

- "Cluster technology felt stuck in the past, when users watched a scrolling
  stream of spots."
- "I wanted a smarter cluster that could provide higher quality spot insight
  and integrate into the N1MM Logger bandmap and Available Mults/Qs window."
- "Quality means two things: can I trust the DX call, and is the path likely
  worth my attention?"
- "The cluster should inform the contester, not make the decision for them."
- "The N1MM bandmap and Available Mults/Qs window are where operators already
  look. Another window would be a distraction."
- "When evidence is insufficient, not showing a tag is a deliberate,
  conservative product decision."
- "I am not a developer. Codex wrote the code while I focused on user
  requirements, direction, and high-level architecture."
- "The surprise was the depth of architecture and planning. Codex became a
  trusted, knowledgeable, objective partner."
- "I want users to feel excited about how AI can drive innovation in ham radio,
  try the cluster, and provide feedback."

## Source Narrative Packet

Use this section as source material, not just guidance. Video and podcast
creators should reuse this language and these examples instead of inventing
generic terminal screens, fake dashboards, or abstract AI visuals.

### Founder Story

The project started from a practical frustration: DX cluster technology still
felt anchored to the old model of watching a scrolling stream of spots. That
model worked when the stream was smaller and the operator could mentally parse
what mattered. It does not fit a modern contest weekend where spot volume is
huge, call quality varies, and operators already have their eyes on N1MM Logger.

The founder was not trying to create another window. Another window would just
pull attention away from the radio. The idea was to build a smarter cluster
from the ground up and put the added intelligence where contesters already
work: the N1MM bandmap and Available Mults/Qs window.

This is also the "Developed with Codex" story. The founder had not written code
since around 1993. He could define the product, user experience, requirements,
and high-level architecture, but Codex made the software possible. Codex wrote
the code, challenged ambiguity, expanded requirements, planned architecture,
created tests, interpreted results, and wrote documentation. The human role was
direction and judgment. Codex turned that direction into a working system.

### User Moment

Picture a contest operator running a frequency. Spots are arriving quickly.
Some are duplicates. Some are busted. Some are correct but probably not worth
leaving the run frequency for. The operator does not have time to study a
separate scrolling window. The decision has to happen inside the workflow they
already use.

The cluster should help answer two questions without taking over the decision:

1. Can I trust this DX call?
2. Is this path likely worth my attention?

The answer is not a command from the cluster. It is a compact signal that helps
the contester apply their own strategy.

### Before And After

Before:

- The operator watches a scrolling spot stream.
- Call correctness is judged manually or by habit.
- Path quality is guessed from experience, propagation intuition, or recent
  luck.
- Useful information is spread across too many places.

After:

- The operator stays in the logger.
- Confidence tags appear where the operator already looks.
- The cluster condenses real-time evidence into compact hints.
- If evidence is insufficient, the cluster stays quiet instead of inventing
  confidence.
- The contester still decides whether to chase the spot.

### Product Beliefs

- The cluster informs; the contester decides.
- No tag is better than a misleading tag.
- Another dashboard is a distraction unless it changes the decision.
- Smarter behavior must not create surprising behavior.
- Trust is built in small details: ordinary spots should not disappear,
  stale evidence should not look current, and support answers should match the
  actual software.
- AI matters here because it lets a domain expert drive a serious software
  project without pretending to be a full-time developer.

### Code-Grounded Spot Examples

These examples were generated from the repository's actual fixed-width spot
formatter, `spot.FormatDXCluster()`, with the path glyph injected using the same
tail-column convention as the telnet layer. Do not replace them with invented
spot streams.

The formatter uses a 78-character DX-cluster line. In the current layout:

- path glyph column: 65
- DX grid column: 67
- call-confidence glyph column: 72
- UTC time column: 74

Use these exact lines for visuals where possible.

#### Spot Without Call Confidence Or Path Tag

This line has no path glyph and no call-confidence glyph. The blank spaces are
intentional.

```text
DX de K1ABC:     14025.00  P5/N1K      CW TNX                     PM37   1843Z
```

Visual meaning:

- DX call: `P5/N1K`
- spotter: `K1ABC`
- frequency: `14025.00`
- mode/comment: `CW TNX`
- DX grid shown: `PM37`
- path tag: blank
- call-confidence tag: blank
- user takeaway: the spot is visible, but the cluster is not claiming call or
  path confidence on this line

#### Spot With Call Confidence And Path Tag

This line has a high path glyph (`>`) and a high-confidence call glyph (`V`).

```text
DX de K1ABC:     14025.00  P5/N1K      CW TNX                   > PM37 V 1843Z
```

Visual meaning:

- path glyph `>`: HIGH, favorable path
- call-confidence glyph `V`: higher-confidence multi-spotter or corroborated
  support, depending on mode path
- DX grid shown: `PM37`
- user takeaway: the cluster is giving the contester two compact signals where
  they already work: the call has stronger support, and the path looks
  favorable

#### Visual Before/After Rule

When showing before/after, use the two real lines above. Do not use random fake
calls, fake scores, or abstract tag boxes as the primary example. If N1MM is
shown, translate these same facts into N1MM-style compact tags beside the same
spot:

- no tags: `P5/N1K 14025.00` with no confidence/path marker
- with tags: `P5/N1K 14025.00  > V`

The exact N1MM rendering may vary, but the meaning must remain grounded in the
real formatter: path glyph plus call-confidence glyph, not generic `[A]` or
`[B]` badges unless the final product actually uses those badges.

### Full Narration Source Draft

Use this as the preferred spoken narrative. The timed scenes below can still
guide pacing and visuals, but this draft gives the creator more human language
to work from.

For a long time, using a DX cluster mostly meant watching a stream of spots
scroll by. During a contest, that stream can move too fast to parse. Some spots
are useful. Some are duplicates. Some are busted calls. And some might be
correct, but still not worth leaving your run frequency for.

That was the starting point for this project. The question was not "how do we
make a prettier cluster?" The question was "how do we make the information more
useful at the exact moment the operator has to decide what to do?"

The answer was not another dashboard. In a contest, another window is often
just another distraction. The operator's eyes are already on the N1MM bandmap
and the Available Mults/Qs window. That is where the intelligence belongs.

So this cluster defines spot quality in two practical ways. First: can I trust
the DX call? During a busy weekend, is this likely to be the right call, or is
it probably busted? Second: is the path worth my attention? If I leave my run
frequency, how likely am I to hear the DX, and how likely is the DX to hear me?

The cluster does not answer that by making the decision for the contester. It
does not say "go chase this." Every operator has a different strategy. The
cluster's job is to provide better evidence, in a compact form, where the
operator is already looking.

Under the hood, the cluster is ingesting a huge real-time observation stream:
upwards of 100,000 spots per minute during heavy periods, many with signal
reports between the two ends of the path. That is a live view of the ionosphere
on a planetary scale. But users should not have to see that complexity. The
cluster organizes the data by geography and band, tracks signal averages, and
condenses the result into simple confidence tags.

The conservative part matters. If there is not enough evidence, the cluster
does not show a path tag. That blank is intentional. A blank is better than a
misleading hint. Future VOACAP-style prediction can help separate "the band is
probably closed" from "we just do not have spots," but the product principle
does not change: do not show confidence unless the evidence supports it.

This is also the story of what AI made possible. The founder of the project was
not a working software developer. He had not written code since around 1993.
His role was to define the user need, the product direction, and the high-level
architecture. Codex handled the development work: requirements, detailed
design, Go code, tests, validation, documentation, and support material.

The surprise was not only speed. It was the depth of planning. Codex acted less
like a coding slave and more like a trusted engineering partner: challenging
ambiguity, finding edge cases, proposing design patterns, and keeping the work
disciplined.

The result is a cluster built for where contesting is going, not just where
cluster technology has been. It puts better information where operators already
look. It respects the operator's judgment. And it is still evolving. Try it,
challenge it, and send feedback, because progress in ham radio comes from
operators using the tools and helping shape what comes next.

## Non-Negotiable Output Rules

The video must:

- Open with the scrolling-spot problem.
- Show N1MM Logger bandmap and Available Mults/Qs as the main user surface.
- Explain spot quality as call confidence plus path confidence.
- Use the exact code-grounded spot examples from this Markdown when showing
  spot lines.
- Present Codex as the enabler of the project, not as a mascot or magic.
- Keep the middle focused on contest decision-making.
- End with an invitation: try it, challenge it, send feedback.

The video must not:

- Become a generic AI software story.
- Become a feature tour.
- Put dashboards or abstract AI visuals at the center.
- Invent fake spot streams when a code-grounded example is available.
- Suggest the cluster decides whether the contester should chase a spot.

## Creator Direction

All generated slides or video scenes must preserve these priorities:

- Primary audience is users. Operators are secondary.
- Lead with user benefit before internal engineering.
- Frame each product section as problem first, then relief. Do not present the
  video as a list of technical features.
- Codex is a central part of the development story, but the user experience is
  still the hero of the product.
- Avoid AI hype, robots, mascots, magic language, or claims that AI replaced
  human judgment.
- Keep the DX cluster visually recognizable: terminal, spots, filters, path
  hints, commands, logs, and support.
- Use practical technical visuals, not abstract futuristic AI art.
- When showing operator tooling, frame it as protecting the user experience.
- When showing the support agent, emphasize faster and more consistent answers,
  not custom GPT technology.
- Human judgment must remain visibly in control.

## Human Voice Rules

The narration should sound like an experienced builder talking to radio users.
Use direct, concrete language. Avoid phrases that sound generated, inflated, or
too symmetrical.

Prefer:

- "Users want useful spots, not noise."
- "A stale hint should not pretend to be current."
- "The support agent helps people find the right answer faster."
- "Codex helped make the work more disciplined."
- "Human judgment still set the direction."

Avoid:

- "unlocking next-generation AI-powered workflows"
- "seamless intelligence layer"
- "revolutionary operational transformation"
- "AI execution core"
- "fake confidence"
- "configuration logs"
- "strict validation tests" when the point is broader validation discipline

## Required Coverage Checklist

The final video or deck must include:

- Classic DX cluster user problem.
- Why the old scrolling-spot model is no longer enough.
- N1MM Logger bandmap and Available Mults/Qs integration.
- Spot quality as two user questions: "Can I trust the DX call?" and "Is the
  path likely worth my attention?"
- Call confidence and propagation/path confidence tags.
- Real-time SNR evidence from high-volume spot ingestion, simplified for users.
- Familiar interface with smarter behavior underneath.
- Predictable filtering, including normal untagged spots staying visible.
- Path reliability with honest uncertainty.
- Nearby/local relevance.
- Practical command example such as `WHOSPOTSME`.
- Operator visibility through diagnostics and event logs.
- AI support agent for consistent answers.
- Codex making the project possible for a non-developer product owner.
- Codex as a trusted, knowledgeable, objective partner, not a coding slave.
- Codex development workflow: inspect, scope, implement, validate, document.
- Human judgment setting product and safety constraints.
- Closing invitation for users to try the cluster and provide feedback.

## Wording Constraints

Prefer:

- "false confidence" over "fake confidence"
- "event logs and diagnostics" over "configuration logs"
- "support users and operators" over "support operators"
- "Codex execution loop" or "AI-assisted work" over "AI execution core"
- "Run validation" or "validate behavior" over "test validations"

Avoid:

- claims that every change had strict validation tests
- implying Codex made product decisions independently
- implying `NEARBY` predicts propagation
- implying path reliability is certain when evidence is stale or insufficient

## Slide Generation Corrections

If this Markdown is used to generate a slide deck, apply these corrections
explicitly:

- Add a dedicated `NEARBY` or local-relevance slide. Do not leave it as a small
  bullet under general filtering.
- Keep human judgment visually outside and above the AI-assisted work. Do not
  put "AI execution core" at the center of the story.
- Label operator-support material as "event logs and diagnostics," not
  "configuration logs."
- Use "support users and operators" when describing the feedback loop.
- When describing Codex, say it helped inspect, challenge, implement, validate,
  document, and support. Do not say or imply it made product decisions alone.
- Show real-feeling DX cluster artifacts: spot lines, filter commands,
  `WHOSPOTSME`, path hints, event logs, support questions.
- Remove any generator watermark, template logo, or tool branding before final
  use unless it is required by the creator tool.

## Scene 1: Opening - Stuck In The Scrolling-Spot Era

Timing: 0:00-0:45

Narration:

For a long time, using a DX cluster mostly meant watching a scrolling stream of
spots. During a busy contest weekend, that stream can move fast. Some spots are
useful. Some are noise. Some may be busted calls. And the operator has only a
few seconds to decide what deserves attention. This project started from a
simple feeling: cluster technology was stuck in the past, and users deserved
something smarter.

Visual direction:

Show a classic terminal or telnet-style DX cluster screen with spots scrolling.
Start with the user's point of view: a fast-moving spot stream during a busy
contest moment. Use repeated variants of the code-grounded examples from this
Markdown rather than random fake calls. Make it feel useful but overloaded, not
broken.

On-screen text:

Beyond the scrolling stream.

## Scene 2: The New Idea - Confidence Where Operators Already Look

Timing: 0:45-1:30

Narration:

The idea was not to build another dashboard. Contest operators already have
their eyes on the N1MM Logger bandmap and Available Mults/Qs window. Another
window would be a distraction. The smarter cluster should put useful confidence
tags directly into those core operating surfaces, so users do not have to watch
a scrolling stream of spots to benefit from better spot intelligence.

Visual direction:

Show the old model as a scrolling cluster window, then shift attention to a
logger bandmap and Available Mults/Qs window with compact confidence tags
appearing beside spots. Use `P5/N1K 14025.00` as the primary example so the
before/after stays tied to the real formatted spot lines.

On-screen text:

Put insight where the operator already works.

## Scene 3: What Higher Quality Spots Means

Timing: 1:30-2:10

Narration:

Higher quality spots means two things from the user's point of view. First, can
I trust the DX call? During a busy contest, how likely is this call to be
correct instead of busted? Second, if I leave my run frequency, how likely am I
to hear the DX, and how likely is the DX to hear me? The cluster does not make
that decision for the contester. It gives better information so each operator
can make the decision according to their own strategy.

Visual direction:

Show two simple decision cards over a contesting screen: "Can I trust the
call?" and "Is the path worth considering?" Keep the focus on operator
decision-making, not math.

On-screen text:

Call confidence. Path confidence.

## Scene 4: Turning Real-Time Evidence Into Simple Hints

Timing: 2:10-2:55

Narration:

The cluster is ingesting a very large stream of spots: upwards of 100,000 spots
per minute during heavy periods, many with signal reports between the two ends
of the path. That becomes a real-time observation of the ionosphere on a
planetary scale. Under the hood, the cluster organizes that evidence by spotter
geography, DX geography, and band, then tracks signal averages for each pair.
Users do not need to see that complexity. They just need a useful hint.

Visual direction:

Show a large stream of incoming spot/SNR observations flowing into geographic
and band buckets, then condensing into one simple confidence tag beside a spot.
End the visual on the exact tagged line:
`DX de K1ABC:     14025.00  P5/N1K      CW TNX                   > PM37 V 1843Z`

On-screen text:

Complex evidence. Simple user signal.

## Scene 5: Conservative By Design

Timing: 2:55-3:40

Narration:

The cluster is deliberately conservative. If the data is insufficient, it does
not show a path tag. That is not a missing feature. It is a product decision.
The cluster should inform the operator, not pretend to know more than it knows.
Over time, VOACAP-style prediction can add a floor, helping distinguish "the
band is probably closed" from "we just do not have spots." But the principle
stays the same: do not show confidence unless the evidence supports it.

Visual direction:

Show three possible outcomes: strong evidence gets a visible tag, insufficient
evidence gets no tag, future prediction floor helps separate closed-band cases
from missing-observation cases. For the insufficient example, use the exact
untagged line:
`DX de K1ABC:     14025.00  P5/N1K      CW TNX                     PM37   1843Z`

On-screen text:

No tag is better than a misleading tag.

## Scene 6: Filtering And Nearby Without Surprises

Timing: 3:40-4:40

Narration:

Smarter behavior also has to avoid surprises. If a user asks for event-related
spots, ordinary untagged spots should not vanish unexpectedly. If a user asks
for nearby relevance, the cluster should treat that as local evidence, not a
magic propagation forecast. These details matter because trust is built in the
small moments. The cluster should help the user focus without taking control
away from them.

Visual direction:

Show filter commands and `PASS NEARBY ON` as user controls. Contrast surprising
behavior with predictable behavior. For nearby, show local station evidence,
not a weather-style propagation map. Do not use heatmaps as the main image.

On-screen text:

Smarter should not mean surprising.

## Scene 7: Useful Commands, Not Dashboard Overload

Timing: 4:40-5:25

Narration:

The main user surface is still the logger, not a new dashboard. That same
principle shapes the command interface. When a user needs a direct answer, the
cluster should answer without pulling them away from the operating flow.
`WHOSPOTSME` is an example: who has recently heard me? It is useful context,
but it stays compact and line-oriented. The feature supports the main idea:
give the operator better information without creating another place to watch.

Visual direction:

Show a user entering `WHOSPOTSME <band>` and receiving a compact summary with
country or region groupings.

On-screen text:

Practical answers inside the familiar command interface.

## Scene 8: Operator Reliability Behind the Scenes

Timing: 5:25-6:10

Narration:

Most users only notice reliability when it fails. That is why operator tools
still matter in a user-first story. They are not the product, but they protect
the product experience. Diagnostics, event logs, and support documentation help
operators understand what happened without dumping noise onto the user's
screen. The user keeps a clean operating view. The operator gets enough signal
to support the system when something needs attention.

Visual direction:

Show user screen remaining clean while a background operator view shows
separate log files and diagnostics.

On-screen text:

Operator visibility protects the user experience.

## Scene 9: AI Support Agent

Timing: 6:10-6:55

Narration:

As the cluster becomes smarter, the questions get smarter too. What does this
tag mean? Why did this spot appear or disappear? Where should I look if
something seems wrong? The support agent helps users and operators find answers
faster, but it is not a separate product story. It supports the same goal:
better decisions with less confusion. The important part is consistency. The
answer should match the behavior of the cluster, the help text, and the
operator documentation.

Visual direction:

Show a user asking a support question, then the answer pointing to command
behavior, filters, logs, or configuration. Keep the focus on faster answers and
consistent explanations.

On-screen text:

AI support helps users get consistent answers faster.

## Scene 10: Codex Made The Project Possible

Timing: 6:55-7:45

Narration:

This project exists because of Codex. The founder was not a working software
developer. The last line of code he had written was around 1993. His role was
to define the user requirements, set the product direction, and guide the
high-level architecture. Codex wrote the cluster code and turned those ideas
into a working system: detailed requirements, architecture, test harnesses,
validation results, documentation, and support material.

Visual direction:

Show two lanes: human product direction and Codex execution. Human lane:
requirements, user experience, architecture. Codex lane: implementation,
testing, documentation, validation, support.

On-screen text:

Human direction. Codex execution.

## Scene 11: Codex As A Trusted Partner

Timing: 7:45-8:25

Narration:

The biggest surprise was not just speed. It was the depth of architecture and
planning. Codex brought design patterns, edge-case thinking, and system
discipline beyond what one person could reasonably hold alone. It was not a
coding slave. It became a trusted, knowledgeable, objective partner: one that
could challenge ambiguity, plan deeply, and keep turning product goals into
checked behavior.

Visual direction:

Show Codex as a disciplined planning and review loop, not as a mascot or robot:
clarify requirements, propose architecture, inspect edge cases, implement,
validate, document.

On-screen text:

A partner, not a coding slave.

## Scene 12: Engineering Discipline

Timing: 8:25-9:15

Narration:

The engineering discipline matters because users experience the result, not the
intent. Codex helped enforce a process: inspect the current state, remove
ambiguity, make scope explicit, implement carefully, validate behavior, update
documentation, and support users and operators. Development moved incredibly
fast, beginning in late November 2025, but the goal was never speed by itself.
The goal was fast progress without losing trust.

Visual direction:

Show a clean engineering loop: inspect, design, implement, validate, document,
review, support users and operators. Use restrained visuals, not hype.

On-screen text:

Better AI results came from stricter engineering.

## Scene 13: Closing - An Invitation

Timing: 9:15-10:00

Narration:

The result is a DX cluster built for where contesting is going, not just where
cluster technology has been. It puts better information where operators already
look. It uses AI-assisted development to turn user ideas into working software.
This is what AI made possible: a radio user could focus on what contesters need,
while Codex helped turn that into a working system. And it is still evolving.
The invitation is simple: try it, challenge it, and send feedback. Progress in
ham radio comes from operators using the tools, sharing ideas, and helping shape
what comes next.

Visual direction:

Return to the DX cluster screen from the opening. Show a clean stream of spots,
a useful command response, the tagged `P5/N1K` example, and a final title card.

On-screen text:

Try it. Challenge it. Help shape what comes next.

## Approximate Timing Summary

- Scene 1: 0:00-0:45
- Scene 2: 0:45-1:30
- Scene 3: 1:30-2:10
- Scene 4: 2:10-2:55
- Scene 5: 2:55-3:40
- Scene 6: 3:40-4:40
- Scene 7: 4:40-5:25
- Scene 8: 5:25-6:10
- Scene 9: 6:10-6:55
- Scene 10: 6:55-7:45
- Scene 11: 7:45-8:25
- Scene 12: 8:25-9:15
- Scene 13: 9:15-10:00

## Suggested Visual Assets

- Terminal-style DX cluster session with spots scrolling, using real formatted
  examples from the `Code-Grounded Spot Examples` section.
- N1MM Logger bandmap and Available Mults/Qs window with compact call/path
  confidence tags.
- Two user decision cards: "Can I trust the DX call?" and "Is the path worth
  my attention?"
- A high-volume spot/SNR evidence stream being simplified into one user-facing
  confidence tag.
- A before/after spot-line comparison:
  - no tags:
    `DX de K1ABC:     14025.00  P5/N1K      CW TNX                     PM37   1843Z`
  - with tags:
    `DX de K1ABC:     14025.00  P5/N1K      CW TNX                   > PM37 V 1843Z`
- Command examples for filtering and `WHOSPOTSME`.
- A `PASS NEARBY ON` or equivalent local-relevance scene that is clearly not a
  propagation forecast.
- Simple diagrams for path evidence: current observations, stale evidence, and
  insufficient confidence.
- Operator-side file logs shown as background support artifacts.
- Documentation and support-agent question examples.
- A final clean contesting view with the message: "Try it. Challenge it. Help
  shape what comes next."

## Production Notes for Video Creator

- Keep the visual style practical and technical, not futuristic or abstract.
- Avoid showing AI as a robot or mascot. Show AI through the workflow: clearer
  decisions, better support, and more consistent behavior.
- Do not over-focus on code. Use code or file visuals briefly as evidence, then
  return to the user benefit.
- Keep on-screen text short. The narration carries the detail.
- The cluster should feel like a real operational tool, not a marketing demo.
- Do not make the narration sound like an advertisement. It should sound like a
  knowledgeable operator/developer explaining the project to other radio users.
- Avoid perfectly balanced slogan pairs on every slide. A few short phrases are
  useful, but too many make the deck feel machine-written.
- Prefer concrete examples over abstractions: spots, filters, commands, stale
  evidence, N1MM bandmap tags, event logs, support answers.
- If a phrase sounds impressive but a normal DX cluster user would not say it,
  rewrite it in plainer language.
- The ending should feel like an invitation to users, not a product victory
  lap.
