# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from sql_metadata.parser import Parser


def test_generalization_of_sql():
    assert Parser(None).generalize is None

    assert (
        Parser("SELECT /* Test */ foo FROM BAR").without_comments
        == "SELECT foo FROM BAR"
    )

    assert (
        Parser(
            "UPDATE  `category` SET cat_pages = cat_pages + 1,cat_files = cat_files + 1 WHERE cat_title = 'foo'"
        ).generalize
        == "UPDATE `category` SET cat_pages = cat_pages + N,cat_files = cat_files + N WHERE cat_title = X"
    )

    assert (
        Parser(
            "SELECT  entity_key  FROM `wall_notification_queue`  WHERE (wiki_id = ) AND (event_date > '20150105141012')"
        ).generalize
        == "SELECT entity_key FROM `wall_notification_queue` WHERE (wiki_id = ) AND (event_date > X)"
    )

    assert (
        Parser(
            "UPDATE  `user` SET user_touched = '20150112143631' WHERE user_id = '25239755'"
        ).generalize
        == "UPDATE `user` SET user_touched = X WHERE user_id = X"
    )

    assert (
        Parser(
            "SELECT /* CategoryDataService::getMostVisited 207.46.13.56 */  page_id,cl_to  FROM `page` INNER JOIN `categorylinks` ON ((cl_from = page_id))  WHERE cl_to = 'Characters' AND (page_namespace NOT IN(500,6,14))  ORDER BY page_title"
        ).generalize
        == "SELECT page_id,cl_to FROM `page` INNER JOIN `categorylinks` ON ((cl_from = page_id)) WHERE cl_to = X AND (page_namespace NOT IN (XYZ)) ORDER BY page_title"
    )

    assert (
        Parser(
            "SELECT /* ArticleCommentList::getCommentList Dancin'NoViolen... */  page_id,page_title  FROM `page`  WHERE (page_title LIKE 'Dreams\\_Come\\_True/@comment-%' ) AND page_namespace = '1'  ORDER BY page_id DESC"
        ).generalize
        == "SELECT page_id,page_title FROM `page` WHERE (page_title LIKE X ) AND page_namespace = X ORDER BY page_id DESC"
    )

    assert (
        Parser(
            "delete /* DatabaseBase::sourceFile( /usr/wikia/slot1/3690/src/maintenance/cleanupStarter.sql ) CreateWiki scri... */ from text where old_id not in (select rev_text_id from revision)"
        ).generalize
        == "delete from text where old_id not in (select rev_text_id from revision)"
    )

    assert (
        Parser(
            "SELECT /* WallNotifications::getBackupData Craftindiedo */  id,is_read,is_reply,unique_id,entity_key,author_id,notifyeveryone  FROM `wall_notification`  WHERE user_id = '24944488' AND wiki_id = '1030786' AND unique_id IN ('880987','882618','708228','522330','662055','837815','792393','341504','600103','612640','667267','482428','600389','213400','620177','164442','659210','621286','609757','575865','567668','398132','549770','495396','344814','421448','400650','411028','341771','379461','332587','314176','284499','250207','231714')  AND is_hidden = '0'  ORDER BY id"
        ).generalize
        == "SELECT id,is_read,is_reply,unique_id,entity_key,author_id,notifyeveryone FROM `wall_notification` WHERE user_id = X AND wiki_id = X AND unique_id IN (XYZ) AND is_hidden = X ORDER BY id"
    )

    # comments with * inside
    assert (
        Parser(
            "SELECT /* ArticleCommentList::getCommentList *Crashie* */  page_id,page_title  FROM `page`  WHERE (page_title LIKE 'Dainava/@comment-%' ) AND page_namespace = '1201'  ORDER BY page_id DESC"
        ).generalize
        == "SELECT page_id,page_title FROM `page` WHERE (page_title LIKE X ) AND page_namespace = X ORDER BY page_id DESC"
    )

    # comments with * inside
    assert (
        Parser(
            "SELECT /* ListusersData::loadData Lart96 - 413bc6e5-b151-44fd-80bd-3baff733fb91 */  count(0) as cnt  FROM `events_local_users`  WHERE wiki_id = '7467' AND (user_name != '') AND user_is_closed = '0' AND ( single_group = 'poweruser' or  all_groups = ''  or  all_groups  LIKE '%bot'  or  all_groups  LIKE '%bot;%'  or  all_groups  LIKE '%bureaucrat'  or  all_groups  LIKE '%bureaucrat;%'  or  all_groups  LIKE '%sysop'  or  all_groups  LIKE '%sysop;%'  or  all_groups  LIKE '%authenticated'  or  all_groups  LIKE '%authenticated;%'  or  all_groups  LIKE '%bot-global'  or  all_groups  LIKE '%bot-global;%'  or  all_groups  LIKE '%content-reviewer'  or  all_groups  LIKE '%content-reviewer;%'  or  all_groups  LIKE '%council'  or  all_groups  LIKE '%council;%'  or  all_groups  LIKE '%fandom-editor'  or  all_groups  LIKE '%fandom-editor;%'  or  all_groups  LIKE '%helper'  or  all_groups  LIKE '%helper;%'  or  all_groups  LIKE '%restricted-login'  or  all_groups  LIKE '%restricted-login;%'  or  all_groups  LIKE '%restricted-login-exempt'  or  all_groups  LIKE '%restricted-login-exempt;%'  or  all_groups  LIKE '%reviewer'  or  all_groups  LIKE '%reviewer;%'  or  all_groups  LIKE '%staff'  or  all_groups  LIKE '%staff;%'  or  all_groups  LIKE '%translator'  or  all_groups  LIKE '%translator;%'  or  all_groups  LIKE '%util'  or  all_groups  LIKE '%util;%'  or  all_groups  LIKE '%vanguard'  or  all_groups  LIKE '%vanguard;%'  or  all_groups  LIKE '%voldev'  or  all_groups  LIKE '%voldev;%'  or  all_groups  LIKE '%vstf'  or  all_groups  LIKE '%vstf;%' ) AND ( edits >= 5)  LIMIT 1  "
        ).generalize
        == "SELECT count(N) as cnt FROM `events_local_users` WHERE wiki_id = X AND (user_name != X) AND user_is_closed = X AND ( single_group = X or all_groups = X or all_groups LIKE X ... ) AND ( edits >= N) LIMIT N"
    )

    # multiline query
    sql = """
    SELECT page_title
        FROM page
        WHERE page_namespace = '10'
        AND page_title COLLATE LATIN1_GENERAL_CI LIKE '%{{Cata%'
            """

    assert (
        Parser(sql).generalize
        == "SELECT page_title FROM page WHERE page_namespace = X AND page_title COLLATE LATINN_GENERAL_CI LIKE X"
    )

    # queries with IN + brackets (#21)
    assert (
        Parser("SELECT foo FROM bar WHERE id IN (123,456, 789)").generalize
        == "SELECT foo FROM bar WHERE id IN (XYZ)"
    )

    assert (
        Parser("SELECT foo FROM bar WHERE id in ( 123, 456, 789 )").generalize
        == "SELECT foo FROM bar WHERE id in (XYZ)"
    )

    assert (
        Parser(
            "SELECT foo FROM bar WHERE slug in (         'american-horror-story', 'animated-series', 'batman', 'comics', 'dc', 'fallout',          'game-of-thrones', 'hbo', 'horror', 'marvel', 'mcu', 'movie-reviews', 'movie-trailers',          'movies', 'netflix', 'playstation', 'star-wars', 'stranger-things', 'streaming',          'the-simpsons', 'zelda'       )"
        ).generalize
        == "SELECT foo FROM bar WHERE slug in (XYZ)"
    )

    assert (
        Parser(
            "select curation_cms.topics.slug from curation_cms.topics where curation_cms.topics.id in (   87, 86, 79, 77, 76, 73, 72, 70, 71, 69, 68, 66, 65, 64, 62, 63, 2, 57, 17, 1,    22, 49, 30, 55, 15, 3, 48, 43, 24, 47, 45, 10, 50, 39, 36, 8, 34, 25, 13, 6, 4 )"
        ).generalize
        == "select curation_cms.topics.slug from curation_cms.topics where curation_cms.topics.id in (XYZ)"
    )


def test_generalize_timestamp():
    assert (
        Parser(
            # ODBC syntax - https://dev.mysql.com/doc/refman/5.7/en/date-and-time-literals.html
            "SELECT foo FROM bar WHERE publish_date < {ts '2018-04-05 10:14:33.824'}"
        ).generalize
        == "SELECT foo FROM bar WHERE publish_date < {ts X}"
    )


def test_generalize_insert():
    assert (
        Parser("INSERT INTO bar (foo, test) Values ( 123, 456, 789 )").generalize
        == "INSERT INTO bar (foo, test) Values (XYZ)"
    )

    assert (
        Parser(
            "/* 7e6384e5 */ insert into notification_stats.request_info (   type,    request_id,    title,    message,    details ) values (   'action-notification',    '51f8a962-bae0-4d25-9341-130658161541',    'RickSanchez15 replied to What''s your overall favourite Season of South Park?.',    'Cool',    'null' )"
        ).generalize
        == "insert into notification_stats.request_info ( type, request_id, title, message, details ) values (XYZ)"
    )
