"""Explicit test-only downgrade of empty-metadata legacy311 state to schema3.

Never silently discard MQTT5 properties or finite deadlines to manufacture a legacy
fixture. Production migrations are owned by mqtt-session, not this test helper.
"""
import copy


def default_subscription(topic, qos):
    return {'filter': topic, 'qos': qos, 'no_local': False,
            'retain_as_published': False, 'retain_handling': 0, 'identifier': None}


def schema6_fixture(value):
    """Remove only proven absent recipient-group metadata from ordinary schema7."""
    result = copy.deepcopy(value)
    if result['schema_version'] == 6:
        return result
    assert result['schema_version'] == 7
    result['schema_version'] = 6
    for session in result['sessions']:
        assert all(not item['filter'].startswith('$share/') for item in session['subscriptions'])
        for message in session['queued']:
            assert message.pop('shared_subscription') is None
        for outgoing in session['outgoing']:
            assert outgoing.pop('shared_subscription') is None
    return result


def schema5_fixture(value):
    """Drop only proven legacy-default subscription/replay state, preserving time/QoS."""
    result = schema6_fixture(value) if value['schema_version'] == 7 else copy.deepcopy(value)
    if result['schema_version'] == 5:
        return result
    assert result['schema_version'] == 6
    result['schema_version'] = 5

    def source_properties(properties):
        assert all('SubscriptionIdentifier' not in prop for prop in properties)

    for session in result['sessions']:
        old = []
        for subscription in session['subscriptions']:
            assert type(subscription['no_local']) is bool
            assert type(subscription['retain_as_published']) is bool
            assert type(subscription['retain_handling']) is int
            assert type(subscription['qos']) is int
            assert subscription == default_subscription(subscription['filter'], subscription['qos'])
            old.append([subscription['filter'], subscription['qos']])
        session['subscriptions'] = old
        for message in session['queued']:
            replay = message.pop('retained_replay')
            assert type(replay) is bool and replay == message['retain']
            source_properties(message['properties'])
        for outgoing in session['outgoing']:
            stage = outgoing['stage']
            retained = False if stage == 'Comp' else bool(next(iter(stage.values()))[0] & 1)
            replay = outgoing.pop('retained_replay')
            assert type(replay) is bool and replay == retained
            source_properties(outgoing['properties'])
    return result


def schema4_fixture(value):
    result = schema5_fixture(value) if value['schema_version'] != 4 else copy.deepcopy(value)
    if result['schema_version'] == 4:
        return result
    assert result['schema_version'] == 5
    result['schema_version'] = 4
    floor = result.pop('clock_floor')
    assert type(floor) is int and 0 <= floor <= 0xffffffffffffffff

    def old_properties(properties):
        for prop in properties:
            assert 'MessageExpiryInterval' not in prop
            assert 'WillDelayInterval' not in prop

    for session in result['sessions']:
        incoming = []
        for identifier, data, properties, deadline in session['incoming']:
            assert deadline is None
            old_properties(properties)
            incoming.append([identifier, data, properties])
        session['incoming'] = incoming
        for outgoing in session['outgoing']:
            assert outgoing.pop('expires_at') is None
            assert outgoing.pop('started') is True
            old_properties(outgoing['properties'])
        for message in session['queued']:
            assert message.pop('expires_at') is None
            old_properties(message['properties'])
    for retained in result['retained']:
        assert retained.pop('expires_at') is None
        old_properties(retained['properties'])
    for will in result['wills']:
        assert will.pop('delay_interval') == 0
        due = will.pop('due_at')
        assert (due is not None) == will['pending']
        old_properties(will['will']['properties'])
    return result


def schema3_fixture(value):
    result = schema4_fixture(value)
    assert result['schema_version'] == 4
    result['schema_version'] = 3
    for session in result['sessions']:
        assert session.pop('session_expiry_interval') == 0xffffffff
        assert session.pop('expires_at') is None
        incoming = []
        for identifier, wire, properties in session['incoming']:
            assert properties == []
            incoming.append([identifier, wire])
        session['incoming'] = incoming
        for outgoing in session['outgoing']:
            assert outgoing.pop('properties') == []
        for message in session['queued']:
            assert message.pop('properties') == []
    for retained in result['retained']:
        assert retained.pop('properties') == []
    for will in result['wills']:
        assert will['will'].pop('properties') == []
    return result
