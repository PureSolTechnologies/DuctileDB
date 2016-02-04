module.exports = function(grunt) {
	var srcDir = 'src/main/'
	grunt.loadNpmTasks('grunt-ts-1.5');
	grunt.loadNpmTasks('grunt-contrib-uglify');
	grunt.loadNpmTasks('grunt-contrib-less');
	grunt.loadNpmTasks('grunt-contrib-cssmin');
	grunt.loadNpmTasks('grunt-contrib-copy');
	grunt.loadNpmTasks('grunt-contrib-htmlmin');
	grunt.loadNpmTasks('grunt-contrib-watch');
	grunt.loadNpmTasks('grunt-tslint');
	grunt
			.initConfig({
				copy : {
					libs : {
						files : [ {
							expand : true,
							src : [ '**' ],
							cwd : 'lib',
							dest : 'target/tmp/lib/'
						}, {
							expand : true,
							src : [ '**' ],
							cwd : 'lib',
							dest : 'target/dist/lib/'
						} ]
					},
					resources : {
						files : [ {
							expand : true,
							src : [ '**' ],
							cwd : 'images/',
							dest : 'target/tmp/images/'
						}, {
							expand : true,
							src : [ '**' ],
							cwd : 'fonts/',
							dest : 'target/tmp/fonts/'
						}, {
							expand : true,
							src : [ '**' ],
							cwd : 'images/',
							dest : 'target/dist/images/'
						}, {
							expand : true,
							src : [ '**' ],
							cwd : 'fonts/',
							dest : 'target/dist/fonts/'
						} ]
					},
					html : {
						files : [ {
							expand : true,
							src : [ '**' ],
							cwd : 'html/',
							dest : 'target/tmp/'
						}, {
							expand : true,
							src : [ '**' ],
							cwd : 'html/',
							dest : 'target/dist/'
						} ]
					},
					javascript : {
						files : [ {
							expand : true,
							src : [ '**' ],
							cwd : 'javascript/',
							dest : 'target/tmp/js'
						} ]
					}
				},
				ts : {
					default : {
						src : [ 'app/**/*.ts' ],
						out : 'target/tmp/js/ductiledb.js',
						options : {
							module : 'amd',
							target : 'es5',
							rootDir : 'typescript',
							watch : false,
							sourceMap : false,
							declaration : false
						}
					}
				},
				less : {
					development : {
						files : {
							'target/dist/css/ductiledb.css' : 'src/main/less/ductiledb.less'
						}
					},
				},
				uglify : {
					target : {
						files : {
							'target/dist/js/ductiledb.js' : 'target/tmp/js/ductiledb.js',
						}
					}
				},
				cssmin : {
					target : {
						files : {
							'target/dist/css/ductiledb.css' : 'target/tmp/css/ductiledb.css'
						}
					}
				},
				htmlmin : {
					production : {
						files : [ {
							expand : true,
							src : [ '**/*.html' ],
							cwd : 'target/tmp/',
							dest : 'target/dist/'
						} ],
						options : {
							removeComments : true,
							removeCommentsFromCDATA : true,
							removeCDATASectionsFromCDATA : true,
							collapseWhitespace : true,
							caseSensitive : true,
							minifyJS : true,
							/*
							 * Do not use removeRedundantAttributes and
							 * minifyCSS! AngularJS/Bootstrap might break!
							 */
							removeRedundantAttributes : false,
							minifyCSS : false
						}
					}
				},
				tslint : {
					options : {
						configuration : grunt.file.readJSON("tslint.json")
					},
					all : {
						src : [ "app/**/*.ts",
								"!app/lib/**/*.ts" ]
					},
				},
				watch : {
					scripts : {
						files : [ 'app/**/*.ts' ],
						tasks : [ 'ts', 'uglify' ],
						options : {
							spawn : false
						}
					},
					styles : {
						files : [ 'less/**/*.less' ],
						tasks : [ 'less', 'cssmin' ],
						options : {
							spawn : false
						}
					},
					html : {
						files : [ 'html/**/*' ],
						tasks : [ 'copy:html', 'htmlmin' ],
						options : {
							spawn : false
						}
					}
				}
			});
	grunt.registerTask('default', 'Builds the whole distribution', [ 'copy',
			'htmlmin', 'ts', 'uglify', 'less', 'cssmin' ]);
}
