import difflib
import jellyfish

def compare_texts(text1, text2, verbosity=0):
    delta = list(difflib.ndiff(text1, text2))

    # lines
    print("lines text 1: {}".format(len(text1)))
    print("lines text 2: {}".format(len(text2)))
    lines_changed = sum(
        [1 if (line.count('+') + line.count('-')) > 0 else 1 / 2 for line in delta if line.startswith('?')])

    lines_added = sum([line.startswith("+") for line in delta]) - lines_changed
    lines_deleted = sum([line.startswith("-") for line in delta]) - lines_changed

    base = len(text1)
    print("deletions: \t{}\t({} %)".format(lines_deleted, 100 * lines_deleted / base))
    print("additions: \t{}\t({} %)".format(lines_added, 100 * lines_added / base))
    print("changes:   \t{}\t({} %)".format(lines_changed, 100 * lines_changed / base))

    # characters
    chars_text1 = sum([len(line) for line in text1])
    chars_text2 = sum([len(line) for line in text2])
    print("\ncharacters text 1: {}".format(chars_text1))
    print("characters text 2: {}".format(chars_text2))
    print("chars diff: {} %".format(100 * (chars_text1 - chars_text2) / chars_text1))

    # chars_added = 0
    # for i in range(len(delta)):
    #     if delta[i].startswith("+") and not delta[i+1].startswith('?'):
    #       chars_added += len(delta[i]) -2

    chars_added = (sum([len(line) - 2 for line in delta if line.startswith("+")])
                   + sum([line.count('+') for line in delta if line.startswith("?")]))

    chars_deleted = (sum([len(line) - 2 for line in delta if line.startswith("-")])
                     + sum([line.count('-') for line in delta if line.startswith("?")]))
    chars_changed = sum([line.count('^') for line in delta if line.startswith("?")])  # / 2

    base = chars_text1
    print("deletions: \t{}\t{}%".format(chars_deleted, 100 * chars_deleted / base))
    print("additions: \t{}\t{}%".format(chars_added, 100 * chars_added / base))
    print("changes: \t{}\t{}%".format(chars_changed, 100 * chars_changed / base))

    if (chars_deleted + chars_added + chars_changed > 0 and verbosity > 0) or verbosity > 1:
        delta = list(difflib.ndiff(text1, text2))
        print("\n================beginning of diff=====================\n")
        print('\n'.join(delta), end="")
        print("\n================end of diff=====================\n")

    diff_score = chars_deleted + chars_added + chars_changed

    return diff_score


# =================================================================================================================

# main execution script for testing
if __name__ == '__main__':

    verbosity = 1

    import django
    import platform

    if platform.node() == "mcc-apsis":
        sys.path.append('/home/muef/tmv/BasicBrowser/')
    else:
        # local paths
        sys.path.append('/media/Data/MCC/tmv/BasicBrowser/')

    # sys.path.append('/home/galm/software/django/tmv/BasicBrowser/')
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "BasicBrowser.settings")
    django.setup()

    # import from appended path
    import parliament.models as pm
    from parliament.tasks import do_search, run_tm
    import cities.models as cmodels
    from django.contrib.auth.models import User
    import tmv_app.models as tm

    # load debate from django database
    wp = 14
    session=1

    parlperiod = pm.ParlPeriod.objects.get(n=wp)

    doc1 = pm.Document.objects.get(sitting=session, parlperiod=parlperiod, text_source__startswith="GermaParlTEI from")
    doc1_id = doc1.id

    content1 = doc1.utterance_set.all().order_by('id').prefetch_related(
        'paragraph_set',
        'paragraph_set__interjection_set',
        'speaker',
        'paragraph_set__interjection_set__persons',
        'paragraph_set__interjection_set__parties')

    doc2 = pm.Document.objects.get(sitting=session, parlperiod=parlperiod,
                                   text_source="from https://www.bundestag.de/service/opendata (scans of pdfs with xml metadata)")
    doc2_id = doc2.id

    content2 = doc2.utterance_set.all().order_by('id').prefetch_related(
        'paragraph_set',
        'paragraph_set__interjection_set',
        'speaker',
        'paragraph_set__interjection_set__persons',
        'paragraph_set__interjection_set__parties')

    i = 0
    for utterance in content1:
        if verbosity > 1:
            print("{} ({}):".format(utterance.speaker.clean_name, utterance.speaker.party))
        utterance2 = content2[i]
        if utterance.speaker.clean_name != utterance2.speaker.clean_name:
            print("speaker not matching: {}, {}".format(utterance.speaker.clean_name, utterance2.speaker.clean_name))

        for par in utterance.paragraph_set.all():
            if verbosity > 1:
                print(par.text)
            if verbosity > 0:
                print(jellyfish.levenshtein_distance(p.text, content2[i].text))

            for ij in par.interjection_set.all():
                if verbosity > 1:
                    try:
                        print("- <interjection by {} ({})> {}".format(
                            ", ".join([person.clean_name for person in ij.persons.all()]),
                            ", ".join([party.name for party in ij.parties.all()]),
                            ij.text))
                    except:
                        print("== Warning: problem printing interjection:")
                        print([person.clean_name for person in ij.persons.all()])
                        print([party.name for party in ij.parties.all()])
                        print(ij.text)
                        pass

        print("<end of utterance>")
