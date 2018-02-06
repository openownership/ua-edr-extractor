# edr_extractor

Whole idea of that project is to parse publically available XML feed of ukrainian
registry of companies and extract the information about beneficiary ownership into
machine readable format (JSONL by default).

Unfortunatelly, the task is not as simple as XML to JSON conversion, and here is why:
The general data format of each record from input feed is following:
<RECORD>
    <NAME></NAME>
    <SHORT_NAME></SHORT_NAME>
    <EDRPOU></EDRPOU>
    <ADDRESS></ADDRESS>
    <BOSS></BOSS>
    <KVED></KVED>
    <STAN></STAN>
    <FOUNDERS>
        <FOUNDER></FOUNDER>
        <FOUNDER></FOUNDER>
        <FOUNDER></FOUNDER>
    </FOUNDERS>
</RECORD>

The information that we need is stored in founders array, where each founder record is a raw string
That record might describe the founder of the company OR beneficial owner (or both!). More over,
beneficial ownership record usually carries not only the name of the owner, but also country of registration
and address (which we also need). In some cases, such record might mention more than one owner or refer to
legal entity as an owner. Finally, some records just saying that there are no beneficial owners or that the founder
is also a beneficial owner (without naming him/her directly)

And here is the pipeline:
*   Loading (opens XML feed downloaded from NAIS and does CP1251 to UTF-8 conversion)
*   Pre-processing (Tokenizes founder records using tokenizer, suitable for ukrainian language)
*   Categorization (Picks founder records which has information about beneficial ownership)
*   Parsing (Extracts the structured information about name/country/address of beneficial owner)
*   Saving (Saves the result in JSONL and CSV format)


To run it you need:
* Publically available export of ukrainian registry of companies (can be obtained from here http://data.gov.ua/passport/73cfe78e-89ef-4f06-b3ab-eb5f16aea237). You'll need only one of 3-4 files from the archive, `*uo*.xml`
* Clone this repo
* Python3.4+
* Install all requirements using pip: `pip -r requirements.txt`
* MITIE models for ML part of the pipeline, which I'm not publishing here, because this margin is too narrow to contain it.
* Make changes to sample.yaml script and then run it like this: `python evaluate.py sample.yaml --show_stats`
* And wait
* Huge CSV file will fall out

Also, I encourage you to read doccomments, code is very thoroughly documented.
