# -*- mode: cperl -*-

use strict;
use warnings;
use CAF::Object;
use Test::Deep;
use Test::More;
use Test::Quattor qw(basic);
use Test::MockModule;
use EDG::WP4::CCM::Element;
use CAF::Application;
use CAF::Reporter;
use AII::Shellfe;

use Readonly;

$CAF::Object::NoAction = 1;

my $mock = Test::MockModule->new('AII::Shellfe');
my $cfg_basic = get_config_for_profile('basic');
my $config_basic = { configuration => $cfg_basic };
my %h = (
    'test01.cluster' => $config_basic,
    'test02.cluster' => $config_basic,
    'test03.cluster' => $config_basic,
    'test04.cluster' => $config_basic,
);

my @opts = qw(script --logfile=target/test/parallel.log --cfgfile=src/test/resources/parallel.cfg);


my $mod = AII::Shellfe->new(@opts);

my ($pm, %responses) = $mod->init_pm('test');
ok(!$pm, 'parallel fork manager not initiated');

my $ok = $mod->status(%h);
diag explain $ok;

$ok = $mod->install(%h);
diag explain $ok;



$mod = AII::Shellfe->new(@opts, "--threads", 2 );
($pm, %responses) = $mod->init_pm('test');
ok($pm, 'parallel fork manager initiated');

$ok = $mod->status(%h);
diag explain $ok;
ok($ok, 'ok is ok');

#@hosts_to_cfg = sort(keys(%res));
#cmp_deeply( \@hosts_to_cfg, \@ALL_HOSTS, 'correct confirmation given' ) ;

$mod = AII::Shellfe->new(@opts, "--threads", 4 );
$ok = $mod->status(%h);
diag explain $ok;


done_testing();
